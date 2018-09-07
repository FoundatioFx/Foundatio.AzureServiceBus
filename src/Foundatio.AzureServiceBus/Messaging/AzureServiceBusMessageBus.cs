using System;
using System.Diagnostics;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Foundatio.Extensions;
using Microsoft.Extensions.Logging;
using Foundatio.Serializer;
using Foundatio.Utility;
using Microsoft.Azure.ServiceBus;
using Microsoft.Azure.ServiceBus.Management;
using Foundatio.AzureServiceBus.Utility;
using Foundatio.AsyncEx;

namespace Foundatio.Messaging {
    public class AzureServiceBusMessageBus : MessageBusBase<AzureServiceBusMessageBusOptions> {
        private readonly AsyncLock _lock = new AsyncLock();
        private TopicClient _topicClient;
        private SubscriptionClient _subscriptionClient;
        private readonly string _subscriptionName;
        private string _tokenValue = String.Empty;
        private DateTime _tokenExpiresAtUtc = DateTime.MinValue;

        public AzureServiceBusMessageBus(AzureServiceBusMessageBusOptions options) : base(options) {
            if (String.IsNullOrWhiteSpace(options.ConnectionString))
                throw new ArgumentException($"{nameof(options.ConnectionString)} is required.");

            if (String.IsNullOrWhiteSpace(options.SubscriptionId))
                throw new ArgumentException($"{nameof(options.SubscriptionId)} is required.");

            _subscriptionName = _options.SubscriptionName ?? MessageBusId;
        }

        public AzureServiceBusMessageBus(Builder<AzureServiceBusMessageBusOptionsBuilder, AzureServiceBusMessageBusOptions> config)
            : this(config(new AzureServiceBusMessageBusOptionsBuilder()).Build()) { }

        protected override async Task EnsureTopicSubscriptionAsync(CancellationToken cancellationToken) {
            if (_subscriptionClient != null)
                return;

            await EnsureTopicCreatedAsync(cancellationToken).AnyContext();

            using (await _lock.LockAsync().AnyContext()) {
                if (_subscriptionClient != null)
                    return;

                var sw = Stopwatch.StartNew();
                try {
                    var sbManagementClient = await GetManagementClient().AnyContext();
                    if (sbManagementClient != null) {
                        await Run.WithRetriesAsync(() => sbManagementClient.Subscriptions.CreateOrUpdateAsync(_options.ResourceGroupName,
                                _options.NameSpaceName, _options.Topic, _subscriptionName,
                                CreateSubscriptionDescription(), cancellationToken),
                            logger: _logger, cancellationToken: cancellationToken).AnyContext();
                    }
                }
                catch (ErrorResponseException e) {
                    if (_logger.IsEnabled(LogLevel.Error)) _logger.LogError(e, "Error creating Topic {SubscriptionName}", _subscriptionName);
                    throw;
                }

                // Look into message factory with multiple recievers so more than one connection is made and managed....
                _subscriptionClient = new SubscriptionClient(_options.ConnectionString, _options.Topic, _subscriptionName, _options.ReceiveMode, _options.SubscriptionRetryPolicy);

                // Enable prefetch to speeden up the receive rate.
                if (_options.PrefetchCount.HasValue)
                    _subscriptionClient.PrefetchCount = _options.PrefetchCount.Value;

                // these are the default values of MessageHandlerOptions
                var maxConcurrentCalls = 1;
                bool autoComplete = true;
                if (_options.MaxConcurrentCalls.HasValue)
                    maxConcurrentCalls = _options.MaxConcurrentCalls.Value;

                if (_options.AutoComplete.HasValue)
                    autoComplete = _options.AutoComplete.Value;

                _subscriptionClient.RegisterMessageHandler(OnMessageAsync, new MessageHandlerOptions(OnExceptionAsync) { MaxConcurrentCalls = maxConcurrentCalls, AutoComplete = autoComplete });
                sw.Stop();
                if (_logger.IsEnabled(LogLevel.Trace)) _logger.LogTrace("Ensure topic subscription exists took {Duration:g}", sw.ElapsedMilliseconds);
            }
        }

        // Use this Handler to look at the exceptions received on the MessagePump
        private Task OnExceptionAsync(ExceptionReceivedEventArgs args) {
            if (_logger.IsEnabled(LogLevel.Error)) _logger.LogError(args.Exception, "Message handler encountered an exception.");
            return Task.CompletedTask ;
        }

        private async Task OnMessageAsync(Message  brokeredMessage, CancellationToken cancellationToken) {
            if (_subscribers.IsEmpty)
                return;

            if (_logger.IsEnabled(LogLevel.Trace)) _logger.LogTrace("Received message: {MessageId} :{SequenceNumber}", brokeredMessage.MessageId, brokeredMessage.SystemProperties.SequenceNumber);
            MessageBusData message;
            try {
                message = _serializer.Deserialize<MessageBusData>(brokeredMessage.Body);
            } catch (Exception ex) {
                if (_logger.IsEnabled(LogLevel.Warning)) _logger.LogWarning(ex, "OnMessageAsync({MessageId}) Error deserializing messsage: {Message}", brokeredMessage.MessageId, ex.Message);
                // A lock token can be found in LockToken, only when ReceiveMode is set to PeekLock
                await _subscriptionClient.DeadLetterAsync(brokeredMessage.SystemProperties.LockToken).AnyContext();
                return;
            }

            if (_options.ReceiveMode == ReceiveMode.PeekLock && _options.AutoComplete == false) {
                await _subscriptionClient.CompleteAsync(brokeredMessage.SystemProperties.LockToken).AnyContext();
            }
            SendMessageToSubscribers(message, _serializer);
            return Task.CompletedTask;
        }

        protected override async Task EnsureTopicCreatedAsync(CancellationToken cancellationToken) {
            if (_topicClient != null)
                return;

            using (await _lock.LockAsync().AnyContext()) {
                if (_topicClient != null)
                    return;

                var sw = Stopwatch.StartNew();
                try {
                    var sbManagementClient = await GetManagementClient().AnyContext();
                    if (sbManagementClient != null) {
                        await Run.WithRetriesAsync(() => sbManagementClient.Topics.CreateOrUpdateAsync(_options.ResourceGroupName,
                                _options.NameSpaceName, _options.Topic, CreateTopicDescription(), cancellationToken),
                            logger: _logger,
                            cancellationToken: cancellationToken).AnyContext();

                    }
                } catch (ErrorResponseException e) {
                    if (_logger.IsEnabled(LogLevel.Error)) _logger.LogError(e, "Error creating {TopicName} Entity", _options.Topic);
                    throw;
                }

                _topicClient = new TopicClient(_options.ConnectionString, _options.Topic);
                sw.Stop();
                if (_logger.IsEnabled(LogLevel.Trace)) _logger.LogTrace("Ensure topic exists took {Duration:g}.", sw.ElapsedMilliseconds);
            }
        }

        protected override async Task PublishImplAsync(string messageType, object message, TimeSpan? delay, CancellationToken cancellationToken) {
            byte[] data = _serializer.SerializeToBytes(new MessageBusData {
                Type = messageType,
                Data = _serializer.SerializeToBytes(message)
            });
            var brokeredMessage = new Message(data) {
                MessageId = Guid.NewGuid().ToString()
            };

            if (delay.HasValue && delay.Value > TimeSpan.Zero) {
                _logger.LogTrace("Schedule delayed message: {MessageType} ({Duration:g})", messageType, delay.Value.TotalMilliseconds);
                brokeredMessage.ScheduledEnqueueTimeUtc = SystemClock.UtcNow.Add(delay.Value);
            } else {
                _logger.LogTrace("Message Publish: {MessageType}", messageType);
            }

            try {
                await _topicClient.SendAsync(brokeredMessage).AnyContext();
            } catch (MessagingEntityNotFoundException e) {
                if (_logger.IsEnabled(LogLevel.Error)) _logger.LogError(e, "Make sure Entity is created");
            }
        }

        private SBTopic CreateTopicDescription() {
            var td = new SBTopic(_options.Topic);

            if (_options.TopicAutoDeleteOnIdle.HasValue)
                td.AutoDeleteOnIdle = _options.TopicAutoDeleteOnIdle.Value;

            if (_options.TopicDefaultMessageTimeToLive.HasValue)
                td.DefaultMessageTimeToLive = _options.TopicDefaultMessageTimeToLive.Value;

            if (_options.TopicMaxSizeInMegabytes.HasValue)
                td.MaxSizeInMegabytes = Convert.ToInt32(_options.TopicMaxSizeInMegabytes.Value);

            if (_options.TopicRequiresDuplicateDetection.HasValue)
                td.RequiresDuplicateDetection = _options.TopicRequiresDuplicateDetection.Value;

            if (_options.TopicDuplicateDetectionHistoryTimeWindow.HasValue)
                td.DuplicateDetectionHistoryTimeWindow = _options.TopicDuplicateDetectionHistoryTimeWindow.Value;

            if (_options.TopicEnableBatchedOperations.HasValue)
                td.EnableBatchedOperations = _options.TopicEnableBatchedOperations.Value;

            //if (_options.TopicEnableFilteringMessagesBeforePublishing.HasValue)
            //    td.EnableFilteringMessagesBeforePublishing = _options.TopicEnableFilteringMessagesBeforePublishing.Value;

            //if (_options.TopicIsAnonymousAccessible.HasValue)
            //    td.IsAnonymousAccessible = _options.TopicIsAnonymousAccessible.Value;

            if (_options.TopicStatus.HasValue)
                td.Status = _options.TopicStatus.Value;

            if (_options.TopicSupportOrdering.HasValue)
                td.SupportOrdering = _options.TopicSupportOrdering.Value;

            if (_options.TopicEnablePartitioning.HasValue)
                td.EnablePartitioning = _options.TopicEnablePartitioning.Value;

            if (_options.TopicEnableExpress.HasValue)
                td.EnableExpress = _options.TopicEnableExpress.Value;

            //if (!String.IsNullOrEmpty(_options.TopicUserMetadata))
            //    td.UserMetadata = _options.TopicUserMetadata;

            return td;
        }

        private SBSubscription CreateSubscriptionDescription() {
            var sd = new SBSubscription(_options.Topic, _subscriptionName);

            if (_options.SubscriptionAutoDeleteOnIdle.HasValue)
                sd.AutoDeleteOnIdle = _options.SubscriptionAutoDeleteOnIdle.Value;

            if (_options.SubscriptionDefaultMessageTimeToLive.HasValue)
                sd.DefaultMessageTimeToLive = _options.SubscriptionDefaultMessageTimeToLive.Value;

            if (_options.SubscriptionWorkItemTimeout.HasValue)
                sd.LockDuration = _options.SubscriptionWorkItemTimeout.Value;

            if (_options.SubscriptionRequiresSession.HasValue)
                sd.RequiresSession = _options.SubscriptionRequiresSession.Value;

            if (_options.SubscriptionEnableDeadLetteringOnMessageExpiration.HasValue)
                sd.DeadLetteringOnMessageExpiration = _options.SubscriptionEnableDeadLetteringOnMessageExpiration.Value;

            // https://github.com/Azure/azure-service-bus-dotnet/issues/255 - Its a bug and should be fixed in the next release.
            //if (_options.SubscriptionEnableDeadLetteringOnFilterEvaluationExceptions.HasValue)
            //    sd.EnableDeadLetteringOnFilterEvaluationExceptions = _options.SubscriptionEnableDeadLetteringOnFilterEvaluationExceptions.Value;

            if (_options.SubscriptionMaxDeliveryCount.HasValue)
                sd.MaxDeliveryCount = _options.SubscriptionMaxDeliveryCount.Value;

            if (_options.SubscriptionEnableBatchedOperations.HasValue)
                sd.EnableBatchedOperations = _options.SubscriptionEnableBatchedOperations.Value;

            if (_options.SubscriptionStatus.HasValue)
                sd.Status = _options.SubscriptionStatus.Value;

            //if (!String.IsNullOrEmpty(_options.SubscriptionForwardTo))
            //    sd.ForwardTo = _options.SubscriptionForwardTo;

            //if (!String.IsNullOrEmpty(_options.SubscriptionForwardDeadLetteredMessagesTo))
            //    sd.ForwardDeadLetteredMessagesTo = _options.SubscriptionForwardDeadLetteredMessagesTo;

            //if (!String.IsNullOrEmpty(_options.SubscriptionUserMetadata))
            //    sd.UserMetadata = _options.SubscriptionUserMetadata;

            return sd;
        }

        public override async void Dispose() {
            base.Dispose();
            await CloseTopicClientAsync();
            await CloseSubscriptionClientAsync();
        }

        private async Task CloseTopicClientAsync() {
            if (_topicClient == null)
                return;

            using (await _lock.LockAsync().AnyContext()) {
                if (_topicClient == null)
                    return;

                await _topicClient.CloseAsync().AnyContext();
                _topicClient = null;
            }
        }

        private async Task CloseSubscriptionClientAsync() {
            if (_subscriptionClient == null)
                return;

            using (await _lock.LockAsync().AnyContext()) {
                if (_subscriptionClient == null)
                    return;

                await _subscriptionClient.CloseAsync().AnyContext();
                _subscriptionClient = null;
            }
        }

        protected virtual async Task<ServiceBusManagementClient> GetManagementClient() {
            var token = await AuthHelper.GetToken(_tokenValue, _tokenExpiresAtUtc, _options.TenantId, _options.ClientId, _options.ClientSecret).AnyContext();
            if (token == null)
                return null;

            _tokenValue = token.TokenValue;
            _tokenExpiresAtUtc = token.TokenExpiresAtUtc;

            var creds = new TokenCredentials(token.TokenValue);
            return new ServiceBusManagementClient(creds) { SubscriptionId = _options.SubscriptionId };
        }
    }
}