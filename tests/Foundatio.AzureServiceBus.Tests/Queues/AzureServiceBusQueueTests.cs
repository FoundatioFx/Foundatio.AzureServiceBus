﻿using System;
using System.Threading.Tasks;
using Foundatio.Queues;
using Foundatio.Tests.Queue;
using Foundatio.Tests.Utility;
using Microsoft.Azure.ServiceBus;
using Microsoft.Extensions.Logging;
using Xunit;
using Xunit.Abstractions;

namespace Foundatio.AzureServiceBus.Tests.Queue;

public class AzureServiceBusQueueTests : QueueTestBase
{
    private readonly string _queueName = "foundatio-" + Guid.NewGuid().ToString("N").Substring(10);

    public AzureServiceBusQueueTests(ITestOutputHelper output) : base(output)
    {
        Log.SetLogLevel<AzureServiceBusQueue<SimpleWorkItem>>(LogLevel.Trace);
    }

    protected override IQueue<SimpleWorkItem> GetQueue(int retries = 1, TimeSpan? workItemTimeout = null, TimeSpan? retryDelay = null, int[] retryMultipliers = null, int deadLetterMaxItems = 100, bool runQueueMaintenance = true, TimeProvider timeProvider = null)
    {
        string connectionString = Configuration.GetConnectionString("AzureServiceBusConnectionString");
        if (String.IsNullOrEmpty(connectionString))
            return null;

        // TODO: Respect retryMultipliers
        var retryPolicy = retryDelay.GetValueOrDefault() > TimeSpan.Zero
            ? new RetryExponential(retryDelay.GetValueOrDefault(), retryDelay.GetValueOrDefault() + retryDelay.GetValueOrDefault(), retries + 1)
            : RetryPolicy.NoRetry;

        _logger.LogDebug("Queue Id: {QueueId}", _queueName);
        return new AzureServiceBusQueue<SimpleWorkItem>(new AzureServiceBusQueueOptions<SimpleWorkItem>
        {
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
            TimeProvider = timeProvider,
            MetricsPollingInterval = TimeSpan.Zero,
            WorkItemTimeout = workItemTimeout.GetValueOrDefault(TimeSpan.FromMinutes(5)),
            LoggerFactory = Log
        });
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
