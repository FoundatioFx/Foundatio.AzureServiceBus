using System;
using System.Collections.Generic;
using System.Globalization;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Azure.Messaging.ServiceBus;
using Foundatio.Messaging;
using Foundatio.Queues;
using Xunit;

namespace Foundatio.AzureServiceBus.Tests;

public class ReceivedPropertyCultureTests
{
    [Theory]
    [InlineData("en-US")]
    [InlineData("de-DE")]
    [InlineData("fr-FR")]
    [InlineData("th-TH")]
    [InlineData("ar-SA")]
    public void Constructor_WithReceivedProperties_UsesInvariantCulture(string cultureName)
    {
        // Arrange
        using var culture = new CultureScope(cultureName);
        using var queue = new InMemoryQueue<string>(o => o.MetricsPollingInterval(TimeSpan.Zero));
        var message = CreateMessage();

        // Act
        var entry = new AzureServiceBusQueueEntry<string>(message, "payload", queue);

        // Assert
        AssertProperties(entry.Properties);
        Assert.Equal(6, entry.Properties.Count);
        Assert.DoesNotContain("CorrelationId", entry.Properties.Keys);
        Assert.DoesNotContain("_attempts", entry.Properties.Keys);
        Assert.Equal("transport-correlation", entry.CorrelationId);
        Assert.Equal(3, entry.Attempts);
        Assert.Same(message, entry.UnderlyingMessage);
        AssertOriginalProperties(message);
    }

    [Theory]
    [InlineData("en-US")]
    [InlineData("de-DE")]
    [InlineData("fr-FR")]
    [InlineData("th-TH")]
    [InlineData("ar-SA")]
    public async Task OnMessageAsync_WithReceivedProperties_UsesInvariantCulture(string cultureName)
    {
        // Arrange
        using var culture = new CultureScope(cultureName);
        await using var bus = new SyntheticMessageBus();
        var message = CreateMessage();
        IMessage? received = null;
        await bus.SubscribeAsync<IMessage>((value, _) =>
        {
            received = value;
            return Task.CompletedTask;
        }, TestContext.Current.CancellationToken);

        // Act
        await bus.ReceiveAsync(message, TestContext.Current.CancellationToken);

        // Assert
        Assert.NotNull(received);
        AssertProperties(received.Properties);
        Assert.Equal(8, received.Properties.Count);
        Assert.Equal("application-correlation", received.Properties["CorrelationId"]);
        Assert.Equal("2", received.Properties["_attempts"]);
        Assert.Equal("transport-correlation", received.CorrelationId);
        Assert.Equal("message-id", received.UniqueId);
        AssertOriginalProperties(message);
    }

    private static void AssertOriginalProperties(ServiceBusReceivedMessage message)
    {
        Assert.Equal(1234.5m, Assert.IsType<decimal>(message.ApplicationProperties["amount"]));
        Assert.Equal(1234.5d, Assert.IsType<double>(message.ApplicationProperties["ratio"]));
        Assert.Equal(new DateTime(2026, 10, 5, 12, 34, 56, DateTimeKind.Utc), Assert.IsType<DateTime>(message.ApplicationProperties["occurredAt"]));
        Assert.Equal(2, Assert.IsType<int>(message.ApplicationProperties["_attempts"]));
        Assert.Null(message.ApplicationProperties["null"]);
        Assert.Equal("trace-id", message.ApplicationProperties["Diagnostic-Id"]);
    }

    private static void AssertProperties(IDictionary<string, string> properties)
    {
        Assert.Equal("1234.5", properties["amount"]);
        Assert.Equal("1234.5", properties["ratio"]);
        Assert.Equal("10/05/2026 12:34:56", properties["occurredAt"]);
        Assert.Equal("001234,50", properties["text"]);
        Assert.Equal(String.Empty, properties["empty"]);
        Assert.Equal("application-label", properties["DiagnosticLabel"]);
        Assert.DoesNotContain("null", properties.Keys);
        Assert.DoesNotContain("Diagnostic-Id", properties.Keys);
        Assert.DoesNotContain("diagnostic-custom", properties.Keys);
    }

    private static ServiceBusReceivedMessage CreateMessage()
    {
        return ServiceBusModelFactory.ServiceBusReceivedMessage(
            body: BinaryData.FromString("payload"),
            messageId: "message-id",
            correlationId: "transport-correlation",
            contentType: "synthetic-message",
            deliveryCount: 1,
            properties: new Dictionary<string, object>
            {
                ["amount"] = 1234.5m,
                ["ratio"] = 1234.5d,
                ["occurredAt"] = new DateTime(2026, 10, 5, 12, 34, 56, DateTimeKind.Utc),
                ["text"] = "001234,50",
                ["empty"] = String.Empty,
                ["null"] = null!,
                ["Diagnostic-Id"] = "trace-id",
                ["diagnostic-custom"] = "trace-custom",
                ["DiagnosticLabel"] = "application-label",
                ["CorrelationId"] = "application-correlation",
                ["_attempts"] = 2
            });
    }

    private sealed class CultureScope : IDisposable
    {
        private readonly CultureInfo _culture = CultureInfo.CurrentCulture;
        private readonly CultureInfo _uiCulture = CultureInfo.CurrentUICulture;

        public CultureScope(string cultureName)
        {
            CultureInfo.CurrentCulture = CultureInfo.GetCultureInfo(cultureName);
            CultureInfo.CurrentUICulture = CultureInfo.GetCultureInfo(cultureName);
        }

        public void Dispose()
        {
            CultureInfo.CurrentCulture = _culture;
            CultureInfo.CurrentUICulture = _uiCulture;
        }
    }

    private sealed class SyntheticMessageBus : AzureServiceBusMessageBus
    {
        public SyntheticMessageBus() : base(new AzureServiceBusMessageBusOptions { ConnectionString = "unused" }) { }

        public Task ReceiveAsync(ServiceBusReceivedMessage message, CancellationToken cancellationToken)
        {
            // Invoke the transport callback without constructing an Azure client or processor.
            var handler = typeof(AzureServiceBusMessageBus)
                .GetMethod("OnMessageAsync", BindingFlags.Instance | BindingFlags.NonPublic)!
                .CreateDelegate<Func<ProcessMessageEventArgs, Task>>(this);
            return handler(new ProcessMessageEventArgs(message, new SyntheticReceiver(), cancellationToken));
        }

        protected override Task EnsureTopicCreatedAsync(CancellationToken cancellationToken) => throw new InvalidOperationException("Synthetic tests cannot create transport infrastructure.");

        protected override Task EnsureTopicSubscriptionAsync(CancellationToken cancellationToken) => Task.CompletedTask;
    }

    private sealed class SyntheticReceiver : ServiceBusReceiver { }
}
