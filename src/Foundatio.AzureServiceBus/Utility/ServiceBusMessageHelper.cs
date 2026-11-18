using System;

namespace Foundatio.AzureServiceBus.Utility;

/// <summary>
/// Internal helper methods for Azure Service Bus message handling.
/// </summary>
internal static class ServiceBusMessageHelper
{
    public const string AttemptsPropertyName = "_attempts";
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
