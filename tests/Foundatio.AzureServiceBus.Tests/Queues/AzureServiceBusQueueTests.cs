using System;
using System.Threading.Tasks;
using Foundatio.Queues;
using Foundatio.Tests.Queue;
using Foundatio.Tests.Utility;
using Microsoft.Extensions.Logging;
using Xunit;

namespace Foundatio.AzureServiceBus.Tests.Queue;

public class AzureServiceBusQueueTests : QueueTestBase
{
    private static readonly bool _isEmulator = IsEmulator();
    private static readonly string _queueName = GetQueueName();

    private static bool IsEmulator()
    {
        string connectionString = Configuration.GetConnectionString("AzureServiceBusConnectionString");
        return !String.IsNullOrEmpty(connectionString) &&
            connectionString.Contains("UseDevelopmentEmulator=true", StringComparison.OrdinalIgnoreCase);
    }

    private static string GetQueueName()
    {
        if (_isEmulator)
            return "foundatio-test-queue";

        return "foundatio-" + Guid.NewGuid().ToString("N")[..10];
    }

    public AzureServiceBusQueueTests(ITestOutputHelper output) : base(output)
    {
        Log.SetLogLevel<AzureServiceBusQueue<SimpleWorkItem>>(LogLevel.Trace);

        // Disable stats assertions when using the emulator since admin API is not available
        if (_isEmulator)
            _assertStats = false;
    }

    protected override IQueue<SimpleWorkItem> GetQueue(int retries = 1, TimeSpan? workItemTimeout = null, TimeSpan? retryDelay = null, int[] retryMultipliers = null, int deadLetterMaxItems = 100, bool runQueueMaintenance = true, TimeProvider timeProvider = null)
    {
        string connectionString = Configuration.GetConnectionString("AzureServiceBusConnectionString");
        if (String.IsNullOrEmpty(connectionString))
            return null;

        var queue = new AzureServiceBusQueue<SimpleWorkItem>(o =>
        {
            o.ConnectionString(connectionString)
             .Name(_queueName)
             .Retries(retries)
             .WorkItemTimeout(workItemTimeout.GetValueOrDefault(TimeSpan.FromMinutes(5)))
             .DequeueInterval(TimeSpan.FromSeconds(1))
             .ReadQueueTimeout(TimeSpan.FromSeconds(5))
             .TimeProvider(timeProvider)
             .MetricsPollingInterval(TimeSpan.Zero)
             .LoggerFactory(Log);

            // These options are not supported by the emulator
            if (!_isEmulator)
            {
                o.AutoDeleteOnIdle(TimeSpan.FromMinutes(5))
                 .EnableBatchedOperations(true)
                 .EnablePartitioning(false) // Disabled to ensure consistent message delivery
                 .RequiresDuplicateDetection(false)
                 .RequiresSession(false);
            }

            return o;
        });

        _logger.LogDebug("Queue Id: {QueueId}", queue.QueueId);
        return queue;
    }

    protected override Task CleanupQueueAsync(IQueue<SimpleWorkItem> queue)
    {
        // Don't delete the queue, it's super expensive and will be cleaned up later.
        queue?.Dispose();
        return Task.CompletedTask;
    }

    [Fact]
    public override Task CanQueueAndDequeueWorkItemAsync()
    {
        return base.CanQueueAndDequeueWorkItemAsync();
    }

    [Fact]
    public override Task CanDequeueWithCancelledTokenAsync()
    {
        return base.CanDequeueWithCancelledTokenAsync();
    }

    [Fact]
    public override Task CanQueueAndDequeueMultipleWorkItemsAsync()
    {
        return base.CanQueueAndDequeueMultipleWorkItemsAsync();
    }

    [Fact]
    public override Task WillWaitForItemAsync()
    {
        return base.WillWaitForItemAsync();
    }

    [Fact]
    public override Task DequeueWaitWillGetSignaledAsync()
    {
        return base.DequeueWaitWillGetSignaledAsync();
    }

    [Fact]
    public override Task CanUseQueueWorkerAsync()
    {
        return base.CanUseQueueWorkerAsync();
    }

    [Fact]
    public override Task CanHandleErrorInWorkerAsync()
    {
        return base.CanHandleErrorInWorkerAsync();
    }

    [Fact(Skip = "Dequeue Time takes forever")]
    public override Task WorkItemsWillTimeoutAsync()
    {
        return base.WorkItemsWillTimeoutAsync();
    }

    [Fact(Skip = "Dequeue Time takes forever")]
    public override Task WillNotWaitForItemAsync()
    {
        return base.WillNotWaitForItemAsync();
    }

    [Fact]
    public override Task WorkItemsWillGetMovedToDeadletterAsync()
    {
        return base.WorkItemsWillGetMovedToDeadletterAsync();
    }

    [Fact(Skip = "Dequeue Time takes forever")]
    public override Task CanResumeDequeueEfficientlyAsync()
    {
        return base.CanResumeDequeueEfficientlyAsync();
    }

    [Fact(Skip = "Dequeue Time takes forever")]
    public override Task CanDequeueEfficientlyAsync()
    {
        return base.CanDequeueEfficientlyAsync();
    }

    [Fact]
    public override Task CanDequeueWithLockingAsync()
    {
        return base.CanDequeueWithLockingAsync();
    }

    [Fact]
    public override Task CanHaveMultipleQueueInstancesWithLockingAsync()
    {
        return base.CanHaveMultipleQueueInstancesWithLockingAsync();
    }

    [Fact]
    public override Task CanAutoCompleteWorkerAsync()
    {
        return base.CanAutoCompleteWorkerAsync();
    }

    [Fact]
    public override Task CanHaveMultipleQueueInstancesAsync()
    {
        return base.CanHaveMultipleQueueInstancesAsync();
    }

    [Fact]
    public override Task CanRunWorkItemWithMetricsAsync()
    {
        return base.CanRunWorkItemWithMetricsAsync();
    }

    [Fact(Skip = "Dequeue Time takes forever")]
    public override Task CanRenewLockAsync()
    {
        return base.CanRenewLockAsync();
    }

    [Fact]
    public override Task CanAbandonQueueEntryOnceAsync()
    {
        return base.CanAbandonQueueEntryOnceAsync();
    }

    [Fact]
    public override Task CanCompleteQueueEntryOnceAsync()
    {
        return base.CanCompleteQueueEntryOnceAsync();
    }

    [Fact(Skip = "Not using this test because you can set specific delay times for servicebus")]
    public override Task CanDelayRetryAsync()
    {
        return base.CanDelayRetryAsync();
    }

    [Fact]
    public override Task VerifyRetryAttemptsAsync()
    {
        return base.VerifyRetryAttemptsAsync();
    }

    [Fact]
    public override Task VerifyDelayedRetryAttemptsAsync()
    {
        return base.VerifyDelayedRetryAttemptsAsync();
    }
}
