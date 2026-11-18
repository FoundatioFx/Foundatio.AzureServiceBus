using System;
using System.Linq;
using Azure.Messaging.ServiceBus;
using Foundatio.AzureServiceBus.Utility;

namespace Foundatio.Queues;

public class AzureServiceBusQueueEntry<T> : QueueEntry<T> where T : class
{
    public ServiceBusReceivedMessage UnderlyingMessage { get; }

    /// <summary>
    /// The session receiver that holds this message's session on session-enabled queues; otherwise null.
    /// </summary>
    internal ServiceBusSessionReceiver? SessionReceiver { get; init; }

    public AzureServiceBusQueueEntry(ServiceBusReceivedMessage message, T value, IQueue<T> queue)
        : base(GetEntryId(message), message.CorrelationId, value, queue, message.EnqueuedTime.UtcDateTime, GetAttemptCount(message))
    {
        if (message.ApplicationProperties is not null)
        {
            foreach (var property in message.ApplicationProperties.Where(a => !IsReservedProperty(a.Key)))
            {
                if (property.Value?.ToString() is { } propValue)
                    Properties.Add(property.Key, propValue);
            }
        }

        UnderlyingMessage = message;
        GroupId = String.IsNullOrEmpty(message.SessionId) ? null : message.SessionId;
    }

    private static bool IsReservedProperty(string propertyName)
    {
        return ServiceBusMessageHelper.IsSdkDiagnosticProperty(propertyName) ||
            propertyName is "CorrelationId" or ServiceBusMessageHelper.AttemptsPropertyName or ServiceBusMessageHelper.OriginalMessageIdPropertyName;
    }

    private static string GetEntryId(ServiceBusReceivedMessage message)
    {
        if (message.ApplicationProperties.TryGetValue(ServiceBusMessageHelper.OriginalMessageIdPropertyName, out object? value) &&
            value is string { Length: > 0 } originalId)
            return originalId;

        return message.MessageId;
    }

    /// <summary>
    /// Gets the attempt count from the message. A scheduled retry copy stores the attempts of the original in the
    /// <c>_attempts</c> application property; deliveries of the copy itself (lock expiry or abandon) add its DeliveryCount.
    /// </summary>
    private static int GetAttemptCount(ServiceBusReceivedMessage message)
    {
        if (message.ApplicationProperties.TryGetValue(ServiceBusMessageHelper.AttemptsPropertyName, out object? attemptsValue) && attemptsValue is int storedAttempts)
            return storedAttempts + message.DeliveryCount;

        // Fall back to delivery count for normal abandon/retry
        return message.DeliveryCount;
    }
}
