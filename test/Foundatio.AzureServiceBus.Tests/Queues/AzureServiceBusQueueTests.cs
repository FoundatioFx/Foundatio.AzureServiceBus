using System;
using System.Diagnostics;
using Foundatio.Queues;
using Foundatio.Tests.Queue;
using Foundatio.Tests.Utility;
using Xunit;
using System.Threading.Tasks;
using Foundatio.AsyncEx;
using Foundatio.Logging;
using Foundatio.Utility;
using Microsoft.Azure.ServiceBus;
using Xunit.Abstractions;

namespace Foundatio.AzureServiceBus.Tests.Queue {
    public class AzureServiceBusQueueTests : QueueTestBase {
        private readonly string _queueName = "foundatio-" + Guid.NewGuid().ToString("N").Substring(10);

        public AzureServiceBusQueueTests(ITestOutputHelper output) : base(output) {
            Log.SetLogLevel<AzureServiceBusQueue<SimpleWorkItem>>(LogLevel.Trace);
        }

        //protected override IQueue<SimpleWorkItem> GetQueue(int retries = 0, TimeSpan? workItemTimeout = null, TimeSpan? retryDelay = null, int deadLetterMaxItems = 100, bool runQueueMaintenance = true) {
        //    string connectionString = Configuration.GetSection("AzureServiceBusConnectionString").Value;
        //    if (String.IsNullOrEmpty(connectionString))
        //        return null;
        //    string clientId = Configuration.GetSection("ClientId").Value;
        //    if (String.IsNullOrEmpty(clientId))
        //        return null;
        //    string tenantId = Configuration.GetSection("TenantId").Value;
        //    if (String.IsNullOrEmpty(tenantId))
        //        return null;
        //    string clientSecret = Configuration.GetSection("ClientSecret").Value;
        //    if (String.IsNullOrEmpty(clientSecret))
        //        return null;
        //    string subscriptionId = Configuration.GetSection("SubscriptionId").Value;
        //    if (String.IsNullOrEmpty(subscriptionId))
        //        return null;
        //    string resourceGroupName = Configuration.GetSection("ResourceGroupName").Value;
        //    if (String.IsNullOrEmpty(resourceGroupName))
        //        return null;
        //    string nameSpaceName = Configuration.GetSection("NameSpaceName").Value;
        //    if (String.IsNullOrEmpty(nameSpaceName))
        //        return null;

        //    var retryPolicy = retryDelay.GetValueOrDefault() > TimeSpan.Zero
        //        ? new RetryExponential(retryDelay.GetValueOrDefault(),
        //        retryDelay.GetValueOrDefault() + retryDelay.GetValueOrDefault(), retries + 1)
        //        : RetryPolicy.Default;

        //    _logger.Debug("Queue Id: {queueId}", _queueName);
        //    return new AzureServiceBusQueue<SimpleWorkItem>(new AzureServiceBusQueueOptions<SimpleWorkItem> {
        //        ConnectionString = connectionString,
        //        Name = _queueName,
        //        AutoDeleteOnIdle = TimeSpan.FromMinutes(5),
        //        EnableBatchedOperations = true,
        //        EnableExpress = true,
        //        EnablePartitioning = true,
        //        SupportOrdering = false,
        //        RequiresDuplicateDetection = false,
        //        RequiresSession = false,
        //        Retries = retries,
        //        RetryPolicy = retryPolicy,
        //        WorkItemTimeout = workItemTimeout.GetValueOrDefault(TimeSpan.FromMinutes(5)),
        //        ClientId = clientId,
        //        TenantId = tenantId,
        //        ClientSecret = clientSecret,
        //        SubscriptionId = subscriptionId,
        //        ResourceGroupName = resourceGroupName,
        //        NameSpaceName =nameSpaceName,
        //        LoggerFactory = Log
        //    });
        //}

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

        private async Task WillWaitItemAsync() {
            var queue = GetQueue();
            if (queue == null)
                return;

            try {
                await queue.DeleteQueueAsync();

                var sw = Stopwatch.StartNew();
                var workItem = await queue.DequeueAsync(TimeSpan.FromMilliseconds(100));
                sw.Stop();
                _logger.Trace("Time {0}", sw.Elapsed);
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
                _logger.Trace("Time {0}", sw.Elapsed);
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
        public override Task WillWaitForItemAsync() {
            return WillWaitItemAsync();
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

        private async Task DontWaitForItemAsync() {
            var queue = GetQueue();
            if (queue == null)
                return;

            try {
                await queue.DeleteQueueAsync();

                var sw = Stopwatch.StartNew();
                var workItem = await queue.DequeueAsync(TimeSpan.FromMilliseconds(100));
                sw.Stop();
                _logger.Trace("Time {0}", sw.Elapsed);
                Assert.Null(workItem);
                Assert.InRange(sw.Elapsed.TotalMilliseconds, 0, 30000);
            }
            finally {
                await CleanupQueueAsync(queue);
            }
        }

        [Fact]
        public override Task WillNotWaitForItemAsync() {
            return DontWaitForItemAsync();
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

        private async Task RenewLockAsync() {
            // Need large value to reproduce this test
            var workItemTimeout = TimeSpan.FromSeconds(30);

            var queue = GetQueue(retryDelay: TimeSpan.Zero, workItemTimeout: workItemTimeout);
            if (queue == null)
                return;

            await queue.EnqueueAsync(new SimpleWorkItem {
                Data = "Hello"
            });
            var entry = await queue.DequeueAsync(TimeSpan.Zero);

            if (entry is QueueEntry<SimpleWorkItem> val) {
                var firstLockedUntilUtcTime = (DateTime) val.Data.GetValueOrDefault("LockedUntilUtc");
                _logger.Trace(() => $"MessageLockedUntil: {firstLockedUntilUtcTime}");
            }

            Assert.NotNull(entry);
            Assert.Equal("Hello", entry.Value.Data);

            _logger.Trace(() => $"Waiting for 5 secs before renewing lock");
            await Task.Delay(TimeSpan.FromSeconds(5));
            _logger.Trace(() => $"Renewing lock");
            await entry.RenewLockAsync();

            //// We shouldn't get another item here if RenewLock works.
            _logger.Trace(() => $"Attempting to dequeue item that shouldn't exist");
            var nullWorkItem = await queue.DequeueAsync(TimeSpan.FromSeconds(2));
            Assert.Null(nullWorkItem);
            await entry.CompleteAsync();
            Assert.Equal(0, (await queue.GetQueueStatsAsync()).Queued);
        }

        [Fact]
        public override Task CanRenewLockAsync() {
            return RenewLockAsync();
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