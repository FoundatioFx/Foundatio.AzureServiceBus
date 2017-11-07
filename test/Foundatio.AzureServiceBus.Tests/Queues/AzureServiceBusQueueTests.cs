using System;
using System.Diagnostics;
using Foundatio.Queues;
using Foundatio.Tests.Queue;
using Xunit;
using System.Threading.Tasks;
using Foundatio.Tests.Utility;
using Foundatio.Utility;
using Microsoft.Azure.ServiceBus;
using Xunit.Abstractions;
using Microsoft.Extensions.Logging;

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

            if (_logger.IsEnabled(LogLevel.Debug)) _logger.LogDebug("Queue Id: {_queueName}", _queueName);
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
                NameSpaceName = nameSpaceName,
                LoggerFactory = Log
            });
        }

        protected override Task CleanupQueueAsync(IQueue<SimpleWorkItem> queue) {
            // Don't delete the queue, it's super expensive and will be cleaned up later.
            queue?.Dispose();
            return Task.CompletedTask;
        }

        [Fact]
        public override Task CanQueueAndDequeueWorkItemAsync() {
            return base.CanQueueAndDequeueWorkItemAsync();
        }

        [Fact]
        public override Task CanDequeueWithCancelledTokenAsync() {
            return base.CanDequeueWithCancelledTokenAsync();
        }

        [Fact]
        public override Task CanQueueAndDequeueMultipleWorkItemsAsync() {
            return base.CanQueueAndDequeueMultipleWorkItemsAsync();
        }

        [Fact]
        public override async Task WillWaitForItemAsync() {
            var queue = GetQueue();
            if (queue == null)
                return;

            try {
                await queue.DeleteQueueAsync();

                var sw = Stopwatch.StartNew();
                var workItem = await queue.DequeueAsync(TimeSpan.FromMilliseconds(100));
                sw.Stop();
                if (_logger.IsEnabled(LogLevel.Trace)) _logger.LogTrace("Time {Elapsed}", sw.Elapsed);
                Assert.Null(workItem);
                Assert.True(sw.Elapsed > TimeSpan.FromMilliseconds(100));

                await Task.Run(async () => {
                    await SystemClock.SleepAsync(500);
                    await queue.EnqueueAsync(new SimpleWorkItem {
                        Data = "Hello"
                    });
                });

                sw.Restart();
                workItem = await queue.DequeueAsync(TimeSpan.FromSeconds(1));
                sw.Stop();
                if (_logger.IsEnabled(LogLevel.Trace)) _logger.LogTrace("Time {Elapsed}", sw.Elapsed);
                // This is varying alot. Sometimes its greater and sometimes its less.
                //Assert.True(sw.Elapsed > TimeSpan.FromMilliseconds(400));
                Assert.NotNull(workItem);
                await workItem.CompleteAsync();

            }
            finally {
                await CleanupQueueAsync(queue);
            }
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

        [Fact]
        public override Task WorkItemsWillTimeoutAsync() {
            return base.WorkItemsWillTimeoutAsync();
        }

        /// <summary>
        /// If run with the base class the result is always:
        /// Range:  (0 - 100)
        /// Actual: 1000.6876
        /// Because base class is using TimeSpan.Zero, the implementation of DequeueAsync changes it to 1 sec.
        /// 1 sec wait for the wait of the item not in the queue is too long for the test case, hence overriding this method so
        /// that DequeueAsync can return with quickly with short timeout.
        /// </summary>
        /// <returns></returns>
        [Fact]
        public override async Task WillNotWaitForItemAsync() {
            var queue = GetQueue();
            if (queue == null)
                return;

            try {
                await queue.DeleteQueueAsync();

                var sw = Stopwatch.StartNew();
                var workItem = await queue.DequeueAsync(TimeSpan.FromMilliseconds(100));
                sw.Stop();
                if (_logger.IsEnabled(LogLevel.Trace)) _logger.LogTrace("Time {Elapsed}", sw.Elapsed);
                Assert.Null(workItem);
                Assert.InRange(sw.Elapsed.TotalMilliseconds, 0, 30000);
            }
            finally {
                await CleanupQueueAsync(queue);
            }
        }

        [Fact]
        public override Task WorkItemsWillGetMovedToDeadletterAsync() {
            return base.WorkItemsWillGetMovedToDeadletterAsync();
        }

        [Fact]
        public override Task CanResumeDequeueEfficientlyAsync() {
            return base.CanResumeDequeueEfficientlyAsync();
        }

        [Fact]
        public override Task CanDequeueEfficientlyAsync() {
            return base.CanDequeueEfficientlyAsync();
        }

        [Fact]
        public override Task CanDequeueWithLockingAsync() {
            return base.CanDequeueWithLockingAsync();
        }

        [Fact]
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

        /// <summary>
        /// This method needs to be overriden because it requires higher LockDuration time 
        /// for the RenewLock. Basically lock for any message needs to be renewed within the time 
        /// period of lockDuration, else MessageLockException gets thrown.
        /// </summary>
        /// <returns></returns>
        [Fact]
        public override async Task CanRenewLockAsync() {
            // Need large value of the lockDuration to reproduce this test
            var workItemTimeout = TimeSpan.FromSeconds(15);

            var queue = GetQueue(retryDelay: TimeSpan.Zero, workItemTimeout: workItemTimeout);
            if (queue == null)
                return;

            await queue.EnqueueAsync(new SimpleWorkItem {
                Data = "Hello"
            });
            var entry = await queue.DequeueAsync(TimeSpan.FromMilliseconds(500));

            if (entry is QueueEntry<SimpleWorkItem> val) {
                var firstLockedUntilUtcTime = (DateTime)val.Data.GetValueOrDefault("LockedUntilUtc");
                if (_logger.IsEnabled(LogLevel.Trace)) _logger.LogTrace("MessageLockedUntil: {firstLockedUntilUtcTime}", firstLockedUntilUtcTime);
            }

            Assert.NotNull(entry);
            Assert.Equal("Hello", entry.Value.Data);

            if (_logger.IsEnabled(LogLevel.Trace)) _logger.LogTrace("Waiting for 5 secs before renewing lock");
            // My observation: If the time to process the item takes longer than the LockDuration,
            // then trying to call RenewLockAsync will give you MessageLockLostException - The lock supplied is invalid. Either the lock expired, or the message has already been removed from the queue. 
            // So this test case is keeping the processing time less than the LockDuration of 30 seconds.
            await Task.Delay(TimeSpan.FromSeconds(2));
            if (_logger.IsEnabled(LogLevel.Trace)) _logger.LogTrace("Renewing lock");
            await entry.RenewLockAsync();

            //// We shouldn't get another item here if RenewLock works.
            if (_logger.IsEnabled(LogLevel.Trace)) _logger.LogTrace("Attempting to dequeue item that shouldn't exist");
            var nullWorkItem = await queue.DequeueAsync(TimeSpan.FromMilliseconds(500));
            Assert.Null(nullWorkItem);
            await entry.CompleteAsync();
            Assert.Equal(0, (await queue.GetQueueStatsAsync()).Queued);
        }

        [Fact]
        public override Task CanAbandonQueueEntryOnceAsync() {
            return base.CanAbandonQueueEntryOnceAsync();
        }

        [Fact]
        public override Task CanCompleteQueueEntryOnceAsync() {
            return base.CanCompleteQueueEntryOnceAsync();
        }

        // NOTE: Not using this test because you can set specific delay times for servicebus
        public override Task CanDelayRetryAsync() {
            return base.CanDelayRetryAsync();
        }
    }
}