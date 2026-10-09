using System;
using System.Linq;
using Azure.Messaging.ServiceBus;
using Foundatio.AzureServiceBus.Utility;

namespace Foundatio.Queues;

public class AzureServiceBusQueueEntry<T> : QueueEntry<T> where T : class
{
    public ServiceBusReceivedMessage UnderlyingMessage { get; }

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
        return ServiceBusMessageHelper.IsSdkDiagnosticProperty(propertyName)
            || propertyName is "CorrelationId" or ServiceBusMessageHelper.AttemptsPropertyName or ServiceBusMessageHelper.OriginalMessageIdPropertyName;
    }

    /// <summary>
    /// Gets the entry id. Scheduled retries on duplicate-detection queues are sent under a new MessageId and carry the
    /// original id in an application property so the entry id stays stable across attempts.
    /// </summary>
    private static string GetEntryId(ServiceBusReceivedMessage message)
    {
        if (message.ApplicationProperties.TryGetValue(ServiceBusMessageHelper.OriginalMessageIdPropertyName, out object? originalId) && originalId is string id && !String.IsNullOrEmpty(id))
            return id;

        return message.MessageId;
    }

    /// <summary>
    /// Gets the attempt count from the message. Uses the _attempts application property if available (for scheduled retries),
    /// otherwise falls back to the DeliveryCount.
    /// </summary>
    private static int GetAttemptCount(ServiceBusReceivedMessage message)
    {
        // Check if we have a stored attempt count from a scheduled retry
        if (message.ApplicationProperties.TryGetValue(ServiceBusMessageHelper.AttemptsPropertyName, out object? attemptsValue) && attemptsValue is int storedAttempts)
            return storedAttempts + 1; // Add 1 because this is a new delivery of that retry

        // Fall back to delivery count for normal abandon/retry
        return message.DeliveryCount;
    }
}
