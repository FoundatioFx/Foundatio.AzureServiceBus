using System;
using Foundatio.Messaging;
using Foundatio.Queues;
using System.Threading.Tasks;
using Foundatio.Tests.Utility;

namespace Foundatio.SenderConsole {
    class Sender {

        private async Task TestTopic() {
            string message;
            IMessageBus messageBus =
                new AzureServiceBusMessageBus(new AzureServiceBusMessageBusOptions() {
                    Topic = "Topic1",
                    ClientId = Configuration.GetSection("ClientId").Value,
                    TenantId = Configuration.GetSection("TenantId").Value,
                    ClientSecret = Configuration.GetSection("ClientSecret").Value,
                    SubscriptionName = "Subscriber1",
                    ConnectionString = Configuration.GetSection("ConnectionString").Value,
                    SubscriptionId = Configuration.GetSection("SubscriptionId").Value,
                    ResourceGroupName = Configuration.GetSection("ResourceGroupName").Value,
                    NameSpaceName = Configuration.GetSection("NameSpaceName").Value,
                    ReceiveMode = Microsoft.Azure.ServiceBus.ReceiveMode.ReceiveAndDelete
                });
            do {
                message = Console.ReadLine();
                await messageBus.PublishAsync(message);
            } while (message != null);

            messageBus.Dispose();
        }

        private async Task TestQueue() {
            string message;
            IQueue<object> queue = new AzureServiceBusQueue<object>(new AzureServiceBusQueueOptions<object>() {
                Name = "queue1",
                ClientId = Configuration.GetSection("ClientId").Value,
                TenantId = Configuration.GetSection("TenantId").Value,
                ClientSecret = Configuration.GetSection("ClientSecret").Value,
                ConnectionString = Configuration.GetSection("ConnectionString").Value,
                SubscriptionId = Configuration.GetSection("SubscriptionId").Value,
                ResourceGroupName = Configuration.GetSection("ResourceGroupName").Value,
                NameSpaceName = Configuration.GetSection("NameSpaceName").Value
            });

            do {
                message = Console.ReadLine();
                await queue.EnqueueAsync(message);
            } while (message != null);
        }

        public Task Run(string[] args) {
            Console.WriteLine("Type your message...");

            return TestTopic();
            //return TestQueue();
        }
    }
}
