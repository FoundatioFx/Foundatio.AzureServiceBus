using System;

namespace Foundatio.AzureServiceBus.Utility;

/// <summary>
/// Internal helper methods for Azure Service Bus message handling.
/// </summary>
internal static class ServiceBusMessageHelper
{
    /// <summary>
    /// Application property that carries the attempt count across scheduled retries.
    /// </summary>
    public const string AttemptsPropertyName = "_attempts";

    /// <summary>
    /// Application property that carries the original MessageId when a scheduled retry is sent under a new id.
    /// </summary>
    public const string OriginalMessageIdPropertyName = "_originalMessageId";

    /// <summary>
    /// Determines if the property is an SDK-added diagnostic property that should be filtered out.
    /// Azure Service Bus SDK adds Diagnostic-Id for internal distributed tracing.
    /// </summary>
    public static bool IsSdkDiagnosticProperty(string propertyName)
    {
        return propertyName.StartsWith("Diagnostic-", StringComparison.OrdinalIgnoreCase);
    }
}
