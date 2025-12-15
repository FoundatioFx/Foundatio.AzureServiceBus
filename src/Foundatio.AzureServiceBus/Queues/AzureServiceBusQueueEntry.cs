using System;
using System.Linq;
using Azure.Messaging.ServiceBus;

namespace Foundatio.Queues;

public class AzureServiceBusQueueEntry<T> : QueueEntry<T> where T : class
{
    public ServiceBusReceivedMessage UnderlyingMessage { get; }

    public AzureServiceBusQueueEntry(ServiceBusReceivedMessage message, T value, IQueue<T> queue)
        : base(message.MessageId, message.CorrelationId, value, queue, message.EnqueuedTime.UtcDateTime, GetAttemptCount(message))
    {
        if (message.ApplicationProperties is not null)
        {
            foreach (var property in message.ApplicationProperties.Where(a => !IsSdkDiagnosticProperty(a.Key) && a.Key != "CorrelationId" && a.Key != "_attempts"))
                Properties.Add(property.Key, property.Value?.ToString());
        }

        UnderlyingMessage = message;
    }

    /// <summary>
    /// Gets the attempt count from the message. Uses the _attempts application property if available (for scheduled retries),
    /// otherwise falls back to the DeliveryCount.
    /// </summary>
    private static int GetAttemptCount(ServiceBusReceivedMessage message)
    {
        // Check if we have a stored attempt count from a scheduled retry
        if (message.ApplicationProperties.TryGetValue("_attempts", out object attemptsValue) && attemptsValue is int storedAttempts)
            return storedAttempts + 1; // Add 1 because this is a new delivery of that retry

        // Fall back to delivery count for normal abandon/retry
        return message.DeliveryCount;
    }

    /// <summary>
    /// Determines if the property is an SDK-added diagnostic property that should be filtered out.
    /// Azure Service Bus SDK adds these for distributed tracing (e.g., Diagnostic-Id, traceparent, tracestate).
    /// </summary>
    private static bool IsSdkDiagnosticProperty(string propertyName)
    {
        return propertyName.StartsWith("Diagnostic-", StringComparison.OrdinalIgnoreCase) ||
               propertyName.Equals("traceparent", StringComparison.OrdinalIgnoreCase) ||
               propertyName.Equals("tracestate", StringComparison.OrdinalIgnoreCase);
    }
}
