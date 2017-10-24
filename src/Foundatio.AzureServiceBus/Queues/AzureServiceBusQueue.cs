using System;
using System.Collections.Generic;
using System.Diagnostics;
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
using Microsoft.Azure.ServiceBus.Core;
using Microsoft.Rest;
using Foundatio.AsyncEx;

namespace Foundatio.Queues {
    public class AzureServiceBusQueue<T> : QueueBase<T, AzureServiceBusQueueOptions<T>> where T : class {
        private readonly AsyncLock _lock = new AsyncLock();
        private readonly MessageReceiver _messageReceiver;
        private QueueClient _queueClient;
        private string _tokenValue = String.Empty;
        private DateTime _tokenExpiresAtUtc = DateTime.MinValue;
        private long _enqueuedCount;
        private long _dequeuedCount;
        private long _completedCount;
        private long _abandonedCount;
        private long _workerErrorCount;

        public AzureServiceBusQueue(AzureServiceBusQueueOptions<T> options) : base(options) {
            if (String.IsNullOrWhiteSpace(options.ConnectionString))
                throw new ArgumentException($"{nameof(options.ConnectionString)} is required.");

            if (String.IsNullOrWhiteSpace(options.SubscriptionId))
                throw new ArgumentException($"{nameof(options.SubscriptionId)} is required.");

            if (options.Name.Length > 260)
                throw new ArgumentException("Queue name must be set and be less than 260 characters.");

            if (options.WorkItemTimeout > TimeSpan.FromMinutes(5))
                throw new ArgumentException("The maximum WorkItemTimeout value for is 5 minutes; the default value is 1 minute.");

            if (options.AutoDeleteOnIdle.HasValue && options.AutoDeleteOnIdle < TimeSpan.FromMinutes(5))
                throw new ArgumentException("The minimum AutoDeleteOnIdle duration is 5 minutes.");

            if (options.DuplicateDetectionHistoryTimeWindow.HasValue && (options.DuplicateDetectionHistoryTimeWindow < TimeSpan.FromSeconds(20.0) || options.DuplicateDetectionHistoryTimeWindow > TimeSpan.FromDays(7.0)))
                throw new ArgumentException("The minimum DuplicateDetectionHistoryTimeWindow duration is 20 seconds and maximum is 7 days.");

            // todo: usermetadata not found in the new lib
            if (options.UserMetadata != null && options.UserMetadata.Length > 260)
                throw new ArgumentException("Queue UserMetadata must be less than 1024 characters.");

            _messageReceiver = new MessageReceiver(_options.ConnectionString, _options.Name);
        }

        protected override async Task EnsureQueueCreatedAsync(CancellationToken cancellationToken = new CancellationToken()) {
            if (_queueClient != null)
                return;

            using (await _lock.LockAsync().AnyContext()) {
                if (_queueClient != null)
                    return;

                var sw = Stopwatch.StartNew();
                try {
                    var sbManagementClient = await GetManagementClient().AnyContext();
                    if (sbManagementClient != null) {
                        await sbManagementClient.Queues.CreateOrUpdateAsync(_options.ResourceGroupName, _options.NameSpaceName, _options.Name, CreateQueueDescription());
                    }
                }
                catch (ServiceBusTimeoutException e) {
                    _logger.Error(e, "Errror while creating the queue");
                }
                catch (ErrorResponseException e) {
                    _logger.Error(e, "Errror while creating the queue");
                }
                catch (Exception e) {
                    _logger.Error(e, "Errror while creating the queue");
                }

                _queueClient = new QueueClient(_options.ConnectionString, _options.Name, ReceiveMode.PeekLock, _options.RetryPolicy);
                sw.Stop();
                _logger.Trace("Ensure queue exists took {0}ms.", sw.ElapsedMilliseconds);
            }
        }

        public override async Task DeleteQueueAsync() {
            var sbManagementClient = await GetManagementClient().AnyContext();
            if (sbManagementClient == null) {
                return;
            }

            await sbManagementClient.Queues.DeleteAsync(_options.ResourceGroupName, _options.NameSpaceName, _options.Name);

            _queueClient = null;
            _enqueuedCount = 0;
            _dequeuedCount = 0;
            _completedCount = 0;
            _abandonedCount = 0;
            _workerErrorCount = 0;
        }

        protected override async Task<QueueStats> GetQueueStatsImplAsync() {

            var sbManagementClient = await GetManagementClient().AnyContext();
            if (sbManagementClient == null) {
                return null;
            }
            var q = await sbManagementClient.Queues.GetAsync(_options.ResourceGroupName, _options.NameSpaceName, _options.Name).AnyContext();
            return new QueueStats {
                Queued = q.MessageCount ?? default(long),
                Working = 0,
                Deadletter = q.CountDetails.DeadLetterMessageCount ?? default(long),
                Enqueued = _enqueuedCount,
                Dequeued = _dequeuedCount,
                Completed = _completedCount,
                Abandoned = _abandonedCount,
                Errors = _workerErrorCount,
                Timeouts = 0
            };
        }

        protected override Task<IEnumerable<T>> GetDeadletterItemsImplAsync(CancellationToken cancellationToken) {
            throw new NotImplementedException();
        }

        protected override async Task<string> EnqueueImplAsync(T data) {
            if (!await OnEnqueuingAsync(data).AnyContext())
                return null;

            Interlocked.Increment(ref _enqueuedCount);
            var message = await _serializer.SerializeAsync(data).AnyContext();
            var brokeredMessage = new Message(message);
            brokeredMessage.MessageId = Guid.NewGuid().ToString();
            await _queueClient.SendAsync(brokeredMessage).AnyContext(); // TODO: See if there is a way to send a batch of messages.
            var entry = new QueueEntry<T>(brokeredMessage.MessageId, data, this, SystemClock.UtcNow, 0);
            await OnEnqueuedAsync(entry).AnyContext();

            return brokeredMessage.MessageId;
        }

        protected override void StartWorkingImpl(Func<IQueueEntry<T>, CancellationToken, Task> handler, bool autoComplete, CancellationToken cancellationToken) {
            if (handler == null)
                throw new ArgumentNullException(nameof(handler));

            var linkedCancellationToken = GetLinkedDisposableCanncellationToken(cancellationToken);

            // TODO: How do you unsubscribe from this or bail out on queue disposed?
            _logger.Trace("WorkerLoop Start {_options.Name}", _options.Name);
            _queueClient.RegisterMessageHandler(async (msg, token) => {
                _logger.Trace("WorkerLoop Signaled {_options.Name}", _options.Name);
                var queueEntry = await HandleDequeueAsync(msg).AnyContext();
                if (queueEntry != null) {
                    var d = queueEntry as QueueEntry<T>;
                    d?.Data.Add("Pull-Strategy", false);
                    d?.Data.Add("LockedUntilUtc", msg.SystemProperties.LockedUntilUtc);

                    try {
                        await handler(queueEntry, linkedCancellationToken).AnyContext();
                        // Handler is registered with AutoComplete as TRUE by default.
                        // todo: autoComplete parameter is not used because - See the Notes below.
                        if (!queueEntry.IsAbandoned && !queueEntry.IsCompleted)
                            await queueEntry.CompleteAsync().AnyContext();
                    }
                    catch (Exception ex) {
                        Interlocked.Increment(ref _workerErrorCount);
                        _logger.Warn(ex, "Error sending work item to worker: {0}", ex.Message);

                        if (!queueEntry.IsAbandoned && !queueEntry.IsCompleted)
                            await queueEntry.AbandonAsync().AnyContext();
                    }
                }
                // AutoComplete is true by default in MessageHandlerOptions. In the old library it used to be false.
                // NOTE AND TEST MORE: if AutoComplete is set to false in the MessageHandlerOptions and we attempt to call
                // CompleteAsync then exception is getting thrown.
                // ex = {"The lock supplied is invalid. Either the lock expired, or the message has already been removed from the queue."}
            }, new MessageHandlerOptions(OnExceptionAsync) { });
        }

        private Task OnExceptionAsync(ExceptionReceivedEventArgs args) {
            _logger.Warn(args.Exception, "Message handler encountered an exception.");
            return Task.CompletedTask;
        }

        public override async Task<IQueueEntry<T>> DequeueAsync(TimeSpan? timeout = null) {
            await EnsureQueueCreatedAsync().AnyContext();
            Message msg;
            if (timeout <= TimeSpan.Zero) {
                // todo: we will be passing min time and max timeout
                _logger.Warn("Azure Service Bus throws Invalid argument exception. Calling ReceiveAsync with 1 secs timeout");
                msg = await _messageReceiver.ReceiveAsync(TimeSpan.FromSeconds(1)).AnyContext();
            }
            else {
                msg = await _messageReceiver.ReceiveAsync(timeout.GetValueOrDefault(TimeSpan.FromSeconds(30)))
                    .AnyContext();
            }
            var queueEntry = await HandleDequeueAsync(msg).AnyContext();
            if (queueEntry != null) {
                var d = queueEntry as QueueEntry<T>;
                d?.Data.Add("Pull-Strategy", true);
                d?.Data.Add("LockedUntilUtc", msg.SystemProperties.LockedUntilUtc);
            }
            return queueEntry;
        }

        protected override Task<IQueueEntry<T>> DequeueImplAsync(CancellationToken cancellationToken) {
            _logger.Warn("Azure Service Bus does not support CancellationTokens - use TimeSpan overload instead. Using default 30 second timeout.");
            return DequeueAsync();
        }

        public override async Task RenewLockAsync(IQueueEntry<T> entry) {
            _logger.Debug("Queue {0} renew lock item: {1}", _options.Name, entry.Id);

            if (entry is QueueEntry<T> val && val.Data["Pull-Strategy"].Equals(true)) {
                var newLockedUntilUtc = await _messageReceiver.RenewLockAsync(entry.Id).AnyContext();
                _logger.Trace($"Renew lock done: { entry.Id} - {newLockedUntilUtc}");
            }

            await OnLockRenewedAsync(entry).AnyContext();
            _logger.Trace("Renew lock done: {0}", entry.Id);
        }

        public override async Task CompleteAsync(IQueueEntry<T> entry) {
            _logger.Debug("Queue {0} complete item: {1}", _options.Name, entry.Id);
            if (entry.IsAbandoned || entry.IsCompleted)
                throw new InvalidOperationException("Queue entry has already been completed or abandoned.");

            // For push strategy AutoComplete is true by default and no need to call ASB CompleteAsync.
            if (entry is QueueEntry<T> val && val.Data["Pull-Strategy"].Equals(true)) {
                await _messageReceiver.CompleteAsync(entry.Id).AnyContext();
            }

            Interlocked.Increment(ref _completedCount);
            entry.MarkCompleted();
            await OnCompletedAsync(entry).AnyContext();
            _logger.Trace("Complete done: {0}", entry.Id);
        }

        public override async Task AbandonAsync(IQueueEntry<T> entry) {
            _logger.Debug("Queue {_options.Name}:{QueueId} abandon item: {entryId}", _options.Name, QueueId, entry.Id);
            if (entry.IsAbandoned || entry.IsCompleted)
                throw new InvalidOperationException("Queue entry has already been completed or abandoned.");

            if (entry is QueueEntry<T> val && val.Data["Pull-Strategy"].Equals(false))
                await _queueClient.AbandonAsync(entry.Id).AnyContext();
            else
                await _messageReceiver.AbandonAsync(entry.Id).AnyContext();
            Interlocked.Increment(ref _abandonedCount);
            entry.MarkAbandoned();
            await OnAbandonedAsync(entry).AnyContext();
            _logger.Trace("Abandon complete: {entryId}", entry.Id);
        }

        private async Task<IQueueEntry<T>> HandleDequeueAsync(Message brokeredMessage) {
            if (brokeredMessage == null)
                return null;

            var message = await _serializer.DeserializeAsync<T>(brokeredMessage.Body).AnyContext();

            Interlocked.Increment(ref _dequeuedCount);
                var entry = new QueueEntry<T>(brokeredMessage.SystemProperties.LockToken, message, this,
                    brokeredMessage.ScheduledEnqueueTimeUtc, brokeredMessage.SystemProperties.DeliveryCount);
            await OnDequeuedAsync(entry).AnyContext();
            return entry;
        }

        private SBQueue CreateQueueDescription() {
            var qd = new SBQueue(_options.Name) {
                LockDuration = _options.WorkItemTimeout,
                MaxDeliveryCount = _options.Retries + 1
            };

            if (_options.AutoDeleteOnIdle.HasValue)
                qd.AutoDeleteOnIdle = _options.AutoDeleteOnIdle.Value;

            if (_options.DefaultMessageTimeToLive.HasValue)
                qd.DefaultMessageTimeToLive = _options.DefaultMessageTimeToLive.Value;

            if (_options.DuplicateDetectionHistoryTimeWindow.HasValue)
                qd.DuplicateDetectionHistoryTimeWindow = _options.DuplicateDetectionHistoryTimeWindow.Value;

            //if (_options.EnableBatchedOperations.HasValue)
            //    qd.EnableBatchedOperations = _options.EnableBatchedOperations.Value;

            if (_options.EnableDeadLetteringOnMessageExpiration.HasValue)
                qd.DeadLetteringOnMessageExpiration = _options.EnableDeadLetteringOnMessageExpiration.Value;

            if (_options.EnableExpress.HasValue)
                qd.EnableExpress = _options.EnableExpress.Value;

            if (_options.EnablePartitioning.HasValue)
                qd.EnablePartitioning = _options.EnablePartitioning.Value;

            //if (!String.IsNullOrEmpty(_options.ForwardDeadLetteredMessagesTo))
            //    qd.ForwardDeadLetteredMessagesTo = _options.ForwardDeadLetteredMessagesTo;

            //if (!String.IsNullOrEmpty(_options.ForwardTo))
            //    qd.ForwardTo = _options.ForwardTo;

            //if (_options.IsAnonymousAccessible.HasValue)
                //qd.IsAnonymousAccessible = _options.IsAnonymousAccessible.Value;

            if (_options.MaxSizeInMegabytes.HasValue)
                qd.MaxSizeInMegabytes = Convert.ToInt32 (_options.MaxSizeInMegabytes.Value);

            if (_options.RequiresDuplicateDetection.HasValue)
                qd.RequiresDuplicateDetection = _options.RequiresDuplicateDetection.Value;

            if (_options.RequiresSession.HasValue)
                qd.RequiresSession = _options.RequiresSession.Value;

            if (_options.Status.HasValue)
                qd.Status = _options.Status.Value;

            //if (_options.SupportOrdering.HasValue)
            //    qd.SupportOrdering = _options.SupportOrdering.Value;

            //if (!String.IsNullOrEmpty(_options.UserMetadata))
            //    qd.UserMetadata = _options.UserMetadata;

            return qd;
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

        public override void Dispose() {
            base.Dispose();
            _queueClient?.CloseAsync();
        }
    }
}