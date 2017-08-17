using System;
using System.Diagnostics;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Foundatio.Extensions;
using Foundatio.Logging;
using Foundatio.Serializer;
using Foundatio.Utility;
using Microsoft.Azure.ServiceBus;
using Foundatio.AzureServiceBus.Utility;
using Microsoft.Azure.Management.ServiceBus;
using Microsoft.Azure.Management.ServiceBus.Models;
using Microsoft.Rest;
using Nito.AsyncEx;

namespace Foundatio.Messaging {
    public class AzureServiceBusMessageBus : MessageBusBase<AzureServiceBusMessageBusOptions> {
        private readonly AsyncLock _lock = new AsyncLock();
        private TopicClient _topicClient;
        private SubscriptionClient _subscriptionClient;
        private readonly string _subscriptionName;
        private string _tokenValue = String.Empty;
        private DateTime _tokenExpiresAtUtc = DateTime.MinValue;

        public AzureServiceBusMessageBus(AzureServiceBusMessageBusOptions options) : base(options) {
            if (String.IsNullOrEmpty(options.ConnectionString))
                throw new ArgumentException("ConnectionString is required.");

            if (String.IsNullOrEmpty(options.SubscriptionId))
                throw new ArgumentException("SubscriptionId is required.");

            _subscriptionName = _options.SubscriptionName ?? MessageBusId;
        }

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
                        await sbManagementClient.Subscriptions.CreateOrUpdateAsync(_options.ResourceGroupName, _options.NameSpaceName,
                            _options.Topic, _subscriptionName, CreateSubscriptionDescription()).AnyContext();
                    }
                }
                catch (ErrorResponseException) { }

                // Look into message factory with multiple recievers so more than one connection is made and managed....
                _subscriptionClient = new SubscriptionClient(_options.ConnectionString, _options.Topic, _subscriptionName, _options.ReceiveMode, _options.SubscriptionRetryPolicy);
                
                // See if this blows up?
                
                // Enable prefetch to speeden up the receive rate.
                if (_options.PrefetchCount.HasValue)
                    _subscriptionClient.PrefetchCount = _options.PrefetchCount.Value;

                // these are the default values of MessageHandlerOptions
                int maxConcurrentCalls = 1;
                bool autoComplete = true;
                if (_options.MaxConcurrentCalls.HasValue)
                    maxConcurrentCalls = _options.MaxConcurrentCalls.Value;

                if (_options.AutoComplete.HasValue)
                    autoComplete = _options.AutoComplete.Value;

                _subscriptionClient.RegisterMessageHandler(OnMessageAsync, new MessageHandlerOptions(OnExceptionAsync) { MaxConcurrentCalls = maxConcurrentCalls, AutoComplete = autoComplete });
                sw.Stop();
                _logger.Trace("Ensure topic subscription exists took {0}ms.", sw.ElapsedMilliseconds);
            }
        }

        // Use this Handler to look at the exceptions received on the MessagePump
        private Task OnExceptionAsync(ExceptionReceivedEventArgs args) {
            _logger.Warn(args.Exception, "Message handler encountered an exception.");
            return Task.CompletedTask ;
        }

        private async Task OnMessageAsync(Message  brokeredMessage, CancellationToken cancellationToken) {
            if (_subscribers.IsEmpty)
                return;

            _logger.Trace($"Received message: messageId:{ brokeredMessage.MessageId} SequenceNumber:{brokeredMessage.SystemProperties.SequenceNumber}");
            MessageBusData message;
            try {
                message = await _serializer.DeserializeAsync<MessageBusData>(brokeredMessage.Body).AnyContext();
            } catch (Exception ex) {
                _logger.Warn(ex, "OnMessageAsync({0}) Error deserializing messsage: {1}", brokeredMessage.MessageId, ex.Message);
                // A lock token can be found in LockToken, only when ReceiveMode is set to PeekLock
                await _subscriptionClient.DeadLetterAsync(brokeredMessage.SystemProperties.LockToken).AnyContext();
                return;
            }

            // NOTES : Please read carefully.
            // There is no need to call CompleteAsync if the receive mode is set to "ReceiveAndDelete".
            // If the ReceiveMode is set to PeekLock and AutoComplete is true then on return of the OnMessageAsync, azure libary takes care of calling CompleteAsync.
            // Please use the below code on the client side only if you intend to pass ReceiveMode as PeekLock and AutoComplete as False.
            // Again, By default the Recieve mode is peek lock and autocomplete is also true. In that case Azure library takes care of completing the message for you
            // as soon as the OnMessage is returned. This holds true for 
            // ReceiveAndDelete option as well.If you have ReceiveMode set ReceiveDelete then there is never a reason to call Message.Complete as the message is
            // already removed from the queue.
            // Caution : This below setting is not recommended because the user may forgot to call CompleteAsync, and then a message would keep appearing,
            // until it is eventually dead-lettered
            //if (_options.ReceiveMode == ReceiveMode.PeekLock && _options.AutoComplete == false) {
            //    await _subscriptionClient.CompleteAsync(brokeredMessage.SystemProperties.LockToken).AnyContext();
            //}
            await SendMessageToSubscribersAsync(message, _serializer).AnyContext();
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
                        await sbManagementClient.Topics.CreateOrUpdateAsync(_options.ResourceGroupName, _options.NameSpaceName, _options.Topic, CreateTopicDescription()).AnyContext();

                    }
                } catch (ErrorResponseException e) {
                    _logger.Error(e, "Error creating Topic Entity");
                    throw e;
                }

                _topicClient = new TopicClient(_options.ConnectionString, _options.Topic);
                sw.Stop();
                _logger.Trace("Ensure topic exists took {0}ms.", sw.ElapsedMilliseconds);
            }
        }

        protected override async Task PublishImplAsync(Type messageType, object message, TimeSpan? delay, CancellationToken cancellationToken) {
            var data = await _serializer.SerializeAsync(new MessageBusData {
                Type = messageType.AssemblyQualifiedName,
                Data = await _serializer.SerializeToStringAsync(message).AnyContext()
            }).AnyContext();

            
            var brokeredMessage = new Message(data);

            if (delay.HasValue && delay.Value > TimeSpan.Zero) {
                _logger.Trace("Schedule delayed message: {messageType} ({delay}ms)", messageType.FullName, delay.Value.TotalMilliseconds);
                brokeredMessage.ScheduledEnqueueTimeUtc = SystemClock.UtcNow.Add(delay.Value);
            } else {
                _logger.Trace("Message Publish: {messageType}", messageType.FullName);
            }

            try {
                await _topicClient.SendAsync(brokeredMessage).AnyContext();
            }
            catch (MessagingEntityNotFoundException e) {
                _logger.Error(e, "Make sure Entity is created");
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

        public override void Dispose() {
            base.Dispose();
            CloseTopicClient();
            CloseSubscriptionClient();
        }

        private async Task CloseTopicClient() {
            if (_topicClient == null)
                return;

            using (await _lock.LockAsync().AnyContext()) {
                if (_topicClient == null)
                    return;

                await _topicClient?.CloseAsync();
                _topicClient = null;
            }
        }

        private async Task CloseSubscriptionClient() {
            if (_subscriptionClient == null)
                return;

            using (await _lock.LockAsync().AnyContext()) {
                if (_subscriptionClient == null)
                    return;

                await _subscriptionClient?.CloseAsync();
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