using System;
using System.Globalization;

namespace Foundatio.AzureServiceBus.Utility;

/// <summary>
/// Internal helper methods for Azure Service Bus message handling.
/// </summary>
internal static class ServiceBusMessageHelper
{
    public static string? FormatPropertyValue(object? value) =>
        value is null ? null : Convert.ToString(value, CultureInfo.InvariantCulture);

    /// <summary>
    /// Determines if the property is an SDK-added diagnostic property that should be filtered out.
    /// Azure Service Bus SDK adds Diagnostic-Id for internal distributed tracing.
    /// </summary>
    public static bool IsSdkDiagnosticProperty(string propertyName)
    {
        return propertyName.StartsWith("Diagnostic-", StringComparison.OrdinalIgnoreCase);
    }
}
