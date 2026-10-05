using System;
using System.Threading.Tasks;
using Foundatio.Queues;
using Foundatio.Tests.Queue;
using Xunit;

namespace Foundatio.AzureServiceBus.Tests.Queues;

public class AzureServiceBusSessionTests
{
    [Fact]
    public async Task DequeueAsync_WithRequiresSession_ThrowsNotSupportedExceptionAsync()
    {
        // Arrange
        await using var queue = CreateQueue();

        // Act
        var result = queue.DequeueAsync(TimeSpan.Zero);

        // Assert
        var exception = await Assert.ThrowsAsync<NotSupportedException>(() => result);
        Assert.Contains("sending with SessionId works", exception.Message);
    }

    [Fact]
    public async Task StartWorkingAsync_WithRequiresSession_ThrowsNotSupportedExceptionAsync()
    {
        // Arrange
        await using var queue = CreateQueue();
        bool handlerCalled = false;

        // Act
        var result = queue.StartWorkingAsync((_, _) =>
        {
            handlerCalled = true;
            return Task.CompletedTask;
        }, cancellationToken: TestContext.Current.CancellationToken);

        // Assert
        await Assert.ThrowsAsync<NotSupportedException>(() => result);
        Assert.False(handlerCalled);
    }

    private static AzureServiceBusQueue<SimpleWorkItem> CreateQueue() => new(o => o
        .ConnectionString("Endpoint=sb://localhost:5672/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=dummy;UseDevelopmentEmulator=true")
        .Name("foundatio-session-guard")
        .RequiresSession(true)
        .MetricsPollingEnabled(false));
}
