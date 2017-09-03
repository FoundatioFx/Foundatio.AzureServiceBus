using System;
using Foundatio.Queues;
using Foundatio.Tests.Queue;
using Foundatio.Tests.Utility;
using Xunit;
using System.Threading.Tasks;
using Foundatio.Logging;
using Microsoft.Azure.ServiceBus;
using Xunit.Abstractions;

namespace Foundatio.AzureServiceBus.Tests.Queue {
    public class AzureServiceBusQueueTests : QueueTestBase {
        private readonly string _queueName = "foundatio-" + Guid.NewGuid().ToString("N").Substring(10);

        public AzureServiceBusQueueTests(ITestOutputHelper output) : base(output) {
            Log.SetLogLevel<AzureServiceBusQueue<SimpleWorkItem>>(LogLevel.Trace);
        }

        protected override IQueue<SimpleWorkItem> GetQueue(int retries = 0, TimeSpan? workItemTimeout = null, TimeSpan? retryDelay = null, int deadLetterMaxItems = 100, bool runQueueMaintenance = true) {
            string connectionString = Configuration.GetSection("AzureServiceBusConnectionString").Value;
            if (String.IsNullOrEmpty(connectionString))
                return null;
            string clientId = Configuration.GetSection("ClientId").Value;
            if (String.IsNullOrEmpty(clientId))
                return null;
            string tenantId = Configuration.GetSection("TenantId").Value;
            if (String.IsNullOrEmpty(tenantId))
                return null;
            string clientSecret = Configuration.GetSection("ClientSecret").Value;
            if (String.IsNullOrEmpty(clientSecret))
                return null;
            string subscriptionId = Configuration.GetSection("SubscriptionId").Value;
            if (String.IsNullOrEmpty(subscriptionId))
                return null;
            string resourceGroupName = Configuration.GetSection("ResourceGroupName").Value;
            if (String.IsNullOrEmpty(resourceGroupName))
                return null;
            string nameSpaceName = Configuration.GetSection("NameSpaceName").Value;
            if (String.IsNullOrEmpty(nameSpaceName))
                return null;

            var retryPolicy = retryDelay.GetValueOrDefault() > TimeSpan.Zero
                ? new RetryExponential(retryDelay.GetValueOrDefault(),
                retryDelay.GetValueOrDefault() + retryDelay.GetValueOrDefault(), retries + 1)
                : RetryPolicy.Default;

            _logger.Debug("Queue Id: {queueId}", _queueName);
            return new AzureServiceBusQueue<SimpleWorkItem>(new AzureServiceBusQueueOptions<SimpleWorkItem> {
                ConnectionString = connectionString,
                Name = _queueName,
                AutoDeleteOnIdle = TimeSpan.FromMinutes(5),
                EnableBatchedOperations = true,
                EnableExpress = true,
                EnablePartitioning = true,
                SupportOrdering = false,
                RequiresDuplicateDetection = false,
                RequiresSession = false,
                Retries = retries,
                RetryPolicy = retryPolicy,
                WorkItemTimeout = workItemTimeout.GetValueOrDefault(TimeSpan.FromMinutes(5)),
                ClientId = clientId,
                TenantId = tenantId,
                ClientSecret = clientSecret,
                SubscriptionId = subscriptionId,
                ResourceGroupName = resourceGroupName,
                NameSpaceName =nameSpaceName,
                LoggerFactory = Log
            });
        }

        protected override Task CleanupQueueAsync(IQueue<SimpleWorkItem> queue) {
            // Don't delete the queue, it's super expensive and will be cleaned up later.
            queue?.Dispose();
            return Task.CompletedTask;
        }

        [Fact(Skip = "ReceiveAsync with timeout zero gives amqp exception. Need more timeout for the receive")]
        public override Task CanQueueAndDequeueWorkItemAsync() {
            return base.CanQueueAndDequeueWorkItemAsync();
        }

        [Fact(Skip="ReceiveAsync with timeout zero gives amqp exception. Need more timeout for the receive")]
        public override Task CanDequeueWithCancelledTokenAsync() {
            return base.CanDequeueWithCancelledTokenAsync();
        }

        [Fact(Skip = "Actual: 7.5221843 time is always coming more than 0-5 range")]
        public override Task CanQueueAndDequeueMultipleWorkItemsAsync() {
            return base.CanQueueAndDequeueMultipleWorkItemsAsync();
        }

        [Fact(Skip = "Dequeue Time for 1 second is not enough")]
        public override Task WillWaitForItemAsync() {
            return base.WillWaitForItemAsync();
        }

        [Fact]
        public override Task DequeueWaitWillGetSignaledAsync() {
            return base.DequeueWaitWillGetSignaledAsync();
        }

        [Fact]
        public override Task CanUseQueueWorkerAsync() {
            return base.CanUseQueueWorkerAsync();
        }

        [Fact]
        public override Task CanHandleErrorInWorkerAsync() {
            return base.CanHandleErrorInWorkerAsync();
        }

        [Fact(Skip = "Timed out trying to create FaultTolerantAmqpObjec")]
        public override Task WorkItemsWillTimeoutAsync() {
            return base.WorkItemsWillTimeoutAsync();
        }

        [Fact(Skip = "Timed out trying to create FaultTolerantAmqpObjec")]
        public override Task WillNotWaitForItemAsync() {
            return base.WillNotWaitForItemAsync();
        }

        [Fact(Skip = "Dequeue Time if set to 5 or 3 seconds work fine wherever you call dequeueasync. Timespan.zero will cause time out exception")]
        public override Task WorkItemsWillGetMovedToDeadletterAsync() {
            return base.WorkItemsWillGetMovedToDeadletterAsync();
        }

        [Fact]
        public override Task CanResumeDequeueEfficientlyAsync() {
            return base.CanResumeDequeueEfficientlyAsync();
        }

        [Fact(Skip ="1 second dequeue returned with 0 items. Need more time to dequeue")]
        public override Task CanDequeueEfficientlyAsync() {
            return base.CanDequeueEfficientlyAsync();
        }

        [Fact(Skip ="todo: task was cancelled")]
        public override Task CanDequeueWithLockingAsync() {
            return base.CanDequeueWithLockingAsync();
        }

        [Fact(Skip = "todo: task was cancelled")]
        public override Task CanHaveMultipleQueueInstancesWithLockingAsync() {
            return base.CanHaveMultipleQueueInstancesWithLockingAsync();
        }

        [Fact]
        public override Task CanAutoCompleteWorkerAsync() {
            return base.CanAutoCompleteWorkerAsync();
        }

        [Fact]
        public override Task CanHaveMultipleQueueInstancesAsync() {
            return base.CanHaveMultipleQueueInstancesAsync();
        }

        [Fact]
        public override Task CanRunWorkItemWithMetricsAsync() {
            return base.CanRunWorkItemWithMetricsAsync();
        }

        [Fact(Skip = "Microsoft.Azure.ServiceBus.ServiceBusTimeoutException if deque is supplied with zero timespan")]
        public override Task CanRenewLockAsync() {
            return base.CanRenewLockAsync();
        }

        [Fact(Skip = "Microsoft.Azure.ServiceBus.ServiceBusTimeoutException if deque is supplied with zero timespan")]
        public override Task CanAbandonQueueEntryOnceAsync() {
            return base.CanAbandonQueueEntryOnceAsync();
        }

        [Fact(Skip = "Microsoft.Azure.ServiceBus.ServiceBusTimeoutException if deque is supplied with zero timespan")]
        public override Task CanCompleteQueueEntryOnceAsync() {
            return base.CanCompleteQueueEntryOnceAsync();
        }

        // NOTE: Not using this test because you can set specific delay times for servicebus
        public override Task CanDelayRetryAsync() {
            return base.CanDelayRetryAsync();
        }
    }
}