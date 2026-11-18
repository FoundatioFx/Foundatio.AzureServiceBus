using System;
using Azure.Messaging.ServiceBus;
using Foundatio.AzureServiceBus.Utility;

namespace Foundatio.Queues;

public class AzureServiceBusQueueEntry<T> : QueueEntry<T> where T : class
{
    private const string AttemptsPropertyName = "_attempts";
    private const string EntryIdAnnotation = "x-opt-foundatio-id";
    private const string PreviewEntryIdAnnotation = "x-opt-foundatio-original-message-id";

    public ServiceBusReceivedMessage UnderlyingMessage { get; }

    /// <summary>
    /// The session receiver that holds this message's session on session-enabled queues; otherwise null.
    /// </summary>
    internal ServiceBusSessionReceiver? SessionReceiver { get; init; }

    public AzureServiceBusQueueEntry(ServiceBusReceivedMessage message, T value, IQueue<T> queue)
        : base(GetEntryId(message), message.CorrelationId, value, queue, message.EnqueuedTime.UtcDateTime, GetAttemptCount(message))
    {
        foreach (var property in message.ApplicationProperties)
        {
            if (String.Equals(property.Key, "CorrelationId", StringComparison.Ordinal) ||
                String.Equals(property.Key, AttemptsPropertyName, StringComparison.Ordinal) ||
                ServiceBusMessageHelper.IsSdkDiagnosticProperty(property.Key))
                continue;

            if (property.Value?.ToString() is { } valueString)
                Properties.Add(property.Key, valueString);
        }

        UnderlyingMessage = message;
        GroupId = String.IsNullOrEmpty(message.SessionId) ? null : message.SessionId;
    }

    internal ServiceBusMessage CreateRetryMessage(DateTimeOffset scheduledEnqueueTime, bool requiresDuplicateDetection)
    {
        var retry = new ServiceBusMessage(UnderlyingMessage) { ScheduledEnqueueTime = scheduledEnqueueTime };
        foreach (string key in UnderlyingMessage.ApplicationProperties.Keys)
        {
            if (ServiceBusMessageHelper.IsSdkDiagnosticProperty(key))
                retry.ApplicationProperties.Remove(key);
        }

        retry.ApplicationProperties[AttemptsPropertyName] = Attempts;
        if (requiresDuplicateDetection)
        {
            // Keep the logical ID separate from caller-owned application properties when the broker ID changes.
            retry.GetRawAmqpMessage().MessageAnnotations[EntryIdAnnotation] = Id;
            retry.MessageId = Guid.NewGuid().ToString("N");
        }

        return retry;
    }

    private static string GetEntryId(ServiceBusReceivedMessage message)
    {
        var annotations = message.GetRawAmqpMessage().MessageAnnotations;
        if ((annotations.TryGetValue(EntryIdAnnotation, out object? value) || annotations.TryGetValue(PreviewEntryIdAnnotation, out value)) &&
            value is string { Length: > 0 } entryId)
            return entryId;

        return message.MessageId;
    }

    /// <summary>
    /// Gets the attempt count from the message. A scheduled retry copy stores the attempts of the original in the
    /// <c>_attempts</c> application property; deliveries of the copy itself (lock expiry or abandon) add its DeliveryCount.
    /// </summary>
    private static int GetAttemptCount(ServiceBusReceivedMessage message)
    {
        if (message.ApplicationProperties.TryGetValue(AttemptsPropertyName, out object? attemptsValue) && attemptsValue is int storedAttempts)
            return storedAttempts + message.DeliveryCount;

        // Fall back to delivery count for normal abandon/retry
        return message.DeliveryCount;
    }
}
