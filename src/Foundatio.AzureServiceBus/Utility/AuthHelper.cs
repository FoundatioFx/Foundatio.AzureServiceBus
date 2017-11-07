using System;
using System.Threading.Tasks;
using Microsoft.IdentityModel.Clients.ActiveDirectory;
namespace Foundatio.AzureServiceBus.Utility
{
    public class TokenModel {
        public string TokenValue { get; set; }
        public DateTime TokenExpiresAtUtc { get; set; }
    }


    public static class AuthHelper {
        public static async Task<TokenModel> GetToken(string tokenValue, DateTime tokenExpiresAtUtc, string tenantId, string clientId, string clientSecret) {
            try {

                if (String.IsNullOrEmpty(tenantId) || String.IsNullOrEmpty(clientId) || String.IsNullOrEmpty(clientSecret)) {
                    return null;
                }

                var tokenModel = new TokenModel() {
                    TokenValue = tokenValue,
                    TokenExpiresAtUtc = tokenExpiresAtUtc
                };
                // Check to see if the token has expired before requesting one.
                // We will go ahead and request a new one if we are within 2 minutes of the token expiring.
                if (tokenExpiresAtUtc < DateTime.UtcNow.AddMinutes(-2) || tokenValue == String.Empty) {
                    Console.WriteLine("Renewing token...");

                    var context = new AuthenticationContext($"https://login.windows.net/{tenantId}");

                    var result = await context.AcquireTokenAsync(
                        "https://management.core.windows.net/",
                        new ClientCredential(clientId, clientSecret)
                    );

                    // If the token isn't a valid string, throw an error.
                    if (String.IsNullOrEmpty(result.AccessToken)) {
                        throw new Exception("Token result is empty!");
                    }

                    tokenModel.TokenExpiresAtUtc = result.ExpiresOn.UtcDateTime;
                    tokenModel.TokenValue = result.AccessToken;

                    Console.WriteLine("Token renewed successfully.");
                }

                return tokenModel;
            }
            catch (Exception e) {
                Console.WriteLine(e);
                throw e;
            }
        }
    }
}
