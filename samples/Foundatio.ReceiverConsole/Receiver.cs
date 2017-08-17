using System;
using Foundatio.Messaging;
using Foundatio.Queues;
using System.Threading.Tasks;
using Foundatio.Tests.Utility;
namespace Foundatio.ReceiverConsole {
    class Receiver {
        private async Task TestTopic() {
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
            await messageBus.SubscribeAsync<string>(msg => { Console.WriteLine(msg); });
            Console.ReadKey();
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
                Console.WriteLine("Waiting to receive messages. Press enter to recv more messages or Ctrl-Z to quit");
                message = Console.ReadLine();
                if (message == null)
                    return;
                try {

                    var stats = await queue.GetQueueStatsAsync();
                    var result = await queue.DequeueAsync(TimeSpan.FromSeconds(5));

                    if (result != null) {
                        Console.WriteLine($"Recieved the message :");
                        await queue.RenewLockAsync(result);
                        await queue.CompleteAsync(result);
                    }
                    var s = await queue.GetQueueStatsAsync();
                }
                catch (Exception e) {
                    Console.WriteLine(e);
                }
            } while (message != null);
        }

        public async Task Run(string[] args) {
            await TestTopic();
            //await TestQueue();
        }
    }
}

