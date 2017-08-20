using System;
using System.Text;
using System.Threading;
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

        private Task GotQueueEntry(IQueueEntry<object> queueEntry, CancellationToken cancellationToken) {
            var msg = queueEntry.Value as Microsoft.Azure.ServiceBus.Message;
            Console.WriteLine($"Recieved the message:  SequenceNumber:{msg.SystemProperties.SequenceNumber} Body: {Encoding.UTF8.GetString(msg.Body)} MessageId : {msg.MessageId}");
            return Task.CompletedTask;
        }

        private async Task TestAutoDeQueue() {
            IQueue<object> queue = new AzureServiceBusQueue<object>(new AzureServiceBusQueueOptions<object>() {
                Name = "queue1",
                ClientId = Configuration.GetSection("ClientId").Value,
                TenantId = Configuration.GetSection("TenantId").Value,
                ClientSecret = Configuration.GetSection("ClientSecret").Value,
                ConnectionString = Configuration.GetSection("ConnectionString").Value,
                SubscriptionId = Configuration.GetSection("SubscriptionId").Value,
                ResourceGroupName = Configuration.GetSection("ResourceGroupName").Value,
                NameSpaceName = Configuration.GetSection("NameSpaceName").Value,
                WorkItemTimeout = TimeSpan.FromMinutes(3)
            });


                try {
                    await queue.StartWorkingAsync(GotQueueEntry, true);
                }
                catch (Exception e) {
                    Console.WriteLine(e);
                }
            Console.ReadKey();
            queue.Dispose();
        }

        private async Task TestDeQueue() {
            string message;
            IQueue<object> queue = new AzureServiceBusQueue<object>(new AzureServiceBusQueueOptions<object>() {
                Name = "queue1",
                ClientId = Configuration.GetSection("ClientId").Value,
                TenantId = Configuration.GetSection("TenantId").Value,
                ClientSecret = Configuration.GetSection("ClientSecret").Value,
                ConnectionString = Configuration.GetSection("ConnectionString").Value,
                SubscriptionId = Configuration.GetSection("SubscriptionId").Value,
                ResourceGroupName = Configuration.GetSection("ResourceGroupName").Value,
                NameSpaceName = Configuration.GetSection("NameSpaceName").Value,
                WorkItemTimeout = TimeSpan.FromMinutes(3)
            });

            do {
                Console.WriteLine("Waiting to receive messages. Press enter to recv more messages or Ctrl-Z to quit");
                message = Console.ReadLine();
                if (message == null)
                    return;
                try {

                    var stats = await queue.GetQueueStatsAsync();
                    Console.WriteLine($"Stats:Dequeued {stats.Dequeued} Enqueued {stats.Enqueued}");
                    var result = await queue.DequeueAsync(TimeSpan.FromSeconds(5));

                    if (result != null) {
                        var msg = result.Value as Microsoft.Azure.ServiceBus.Message;
                        await queue.CompleteAsync(result);
                        Console.WriteLine($"Recieved the message:  SequenceNumber:{msg.SystemProperties.SequenceNumber} Body: {Encoding.UTF8.GetString (msg.Body)} MessageId : {msg.MessageId}");
                    }
                    stats = await queue.GetQueueStatsAsync();
                    Console.WriteLine($"Stats:Dequeued {stats.Dequeued} Enqueued {stats.Enqueued}");
                }
                catch (Exception e) {
                    Console.WriteLine(e);
                }
            } while (message != null);
        }

        public async Task Run(string[] args) {
            await TestTopic();
            await TestDeQueue();
            await TestAutoDeQueue();
            
        }
    }
}

