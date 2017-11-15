using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading.Tasks;
using Exceptionless;
using Foundatio.AsyncEx;

using Foundatio.Tests.Utility;
using Foundatio.Messaging;
using Foundatio.Tests.Extensions;
using Foundatio.Tests.Messaging;
using Foundatio.Utility;
using Microsoft.Azure.ServiceBus;
using Microsoft.Extensions.Logging;
using Xunit;
using Xunit.Abstractions;

namespace Foundatio.AzureServiceBus.Tests.Messaging {
    public class AzureServiceBusMessageBusTests : MessageBusTestBase {
        public AzureServiceBusMessageBusTests(ITestOutputHelper output) : base(output) {}

        protected override IMessageBus GetMessageBus() {
            string connectionString = Configuration.GetSection("AzureServiceBusConnectionString").Value;
            if (String.IsNullOrEmpty(connectionString))
                return null;

            return new AzureServiceBusMessageBus(new AzureServiceBusMessageBusOptions {
                    Topic = "test-messages",
                    ClientId = Configuration.GetSection("ClientId").Value,
                    TenantId = Configuration.GetSection("TenantId").Value,
                    ClientSecret = Configuration.GetSection("ClientSecret").Value,
                    ConnectionString = connectionString,
                    SubscriptionId = Configuration.GetSection("SubscriptionId").Value,
                    ResourceGroupName = Configuration.GetSection("ResourceGroupName").Value,
                    NameSpaceName = Configuration.GetSection("NameSpaceName").Value,
                    ReceiveMode = ReceiveMode.ReceiveAndDelete,
                    SubscriptionWorkItemTimeout = TimeSpan.FromMinutes(5),
                    AutoComplete = false,
                    TopicEnableBatchedOperations = true,
                    TopicEnableExpress = true,
                    TopicEnablePartitioning = true,
                    TopicSupportOrdering = false,
                    TopicRequiresDuplicateDetection = false,
                    SubscriptionAutoDeleteOnIdle = TimeSpan.FromMinutes(5),
                    SubscriptionEnableBatchedOperations = true,
                    SubscriptionMaxDeliveryCount = int.MaxValue,
                    PrefetchCount = 500,
                    LoggerFactory = Log
            });
        }

        
        protected IMessageBus GetMessageBus(ReceiveMode mode, TimeSpan subscriptionWorkItemTimeout,
            bool autoComplete, bool topicEnableBatchedOperations, bool topicEnableExpress, bool topicEnablePartitioning,
            bool topicSupportOrdering, bool topicRequiresDuplicateDetection, TimeSpan subscriptionAutoDeleteOnIdle,
            bool subscriptionEnableBatchedOperations, int subscriptionMaxDeliveryCount,
            int prefetchCount) {
            string connectionString = Configuration.GetSection("AzureServiceBusConnectionString").Value;
            if (String.IsNullOrEmpty(connectionString))
                return null;

            return new AzureServiceBusMessageBus(new AzureServiceBusMessageBusOptions {
                Topic = "test-messages",
                ClientId = Configuration.GetSection("ClientId").Value,
                TenantId = Configuration.GetSection("TenantId").Value,
                ClientSecret = Configuration.GetSection("ClientSecret").Value,
                ConnectionString = connectionString,
                SubscriptionId = Configuration.GetSection("SubscriptionId").Value,
                ResourceGroupName = Configuration.GetSection("ResourceGroupName").Value,
                NameSpaceName = Configuration.GetSection("NameSpaceName").Value,
                ReceiveMode = mode,
                SubscriptionWorkItemTimeout = subscriptionWorkItemTimeout,
                AutoComplete = autoComplete,
                TopicEnableBatchedOperations = topicEnableBatchedOperations,
                TopicEnableExpress = topicEnableExpress,
                TopicEnablePartitioning = topicEnablePartitioning,
                TopicSupportOrdering = topicSupportOrdering,
                TopicRequiresDuplicateDetection = topicRequiresDuplicateDetection,
                SubscriptionAutoDeleteOnIdle = subscriptionAutoDeleteOnIdle,
                SubscriptionEnableBatchedOperations = subscriptionEnableBatchedOperations,
                SubscriptionMaxDeliveryCount = subscriptionMaxDeliveryCount,
                PrefetchCount = prefetchCount,
                LoggerFactory = Log
            });
        }


        [Fact]
        public override Task CanSendMessageAsync() {
            return base.CanSendMessageAsync();
        }

        [Fact]
        public override Task CanHandleNullMessageAsync() {
            return base.CanHandleNullMessageAsync();
        }

        [Fact]
        public override Task CanSendDerivedMessageAsync() {
            return base.CanSendDerivedMessageAsync();
        }

        [Fact(Skip = "This method is failing because subscribers are not getting called in the said timed duration.")]
        public override Task CanSendDelayedMessageAsync() {
            return base.CanSendDelayedMessageAsync();
        }

        [Fact]
        public async Task CanSendDelayed2MessagesAsync() {
            var messageBus = GetMessageBus();
            if (messageBus == null)
                return;

            try {
                var countdown = new AsyncCountdownEvent(2);

                int messages = 0;
                await messageBus.SubscribeAsync<SimpleMessageA>(msg => {
                    if (++messages % 2 == 0)
                        Console.WriteLine("subscribed 2 messages...");
                    Assert.Equal("Hello", msg.Data);
                    countdown.Signal();
                });

                var sw = Stopwatch.StartNew();
                await Run.InParallelAsync(2, async i => {
                    await messageBus.PublishAsync(new SimpleMessageA {
                        Data = "Hello",
                        Count = i
                    }, TimeSpan.FromMilliseconds(RandomData.GetInt(0, 100)));
                    if (i % 2 == 0)
                        Console.WriteLine( "Published 2 messages...");
                });

                await countdown.WaitAsync(TimeSpan.FromSeconds(50));
                sw.Stop();

                if (_logger.IsEnabled(LogLevel.Trace)) _logger.LogTrace("Processed {TestCurrentCount} in {Duration:g}", 2 - countdown.CurrentCount, sw.ElapsedMilliseconds);
                Assert.Equal(0, countdown.CurrentCount);
            }
            finally {
                await CleanupMessageBusAsync(messageBus);
            }
        }

        [Fact]
        public override Task CanSubscribeConcurrentlyAsync() {
            return base.CanSubscribeConcurrentlyAsync();
        }

        /// <summary>
        /// This method demonstrates that Message Bus Topic is very slow in performance
        /// if used with ReceiveMode as PeekAndLock. One should create subscription client with RecieveMode as ReceiveAndDelete
        /// for way better performance.
        /// </summary>
        /// <returns></returns>
        [Fact]
        public async Task CanSlowSubscribeConcurrentlyAsync() {
            const int iterations = 100;
            var messageBus = GetMessageBus(ReceiveMode.PeekLock, TimeSpan.FromMinutes(5), false,
                true, true, true, false, false, TimeSpan.FromMinutes(5), true, int.MaxValue, 500);
            if (messageBus == null)
                return;

            try {
                var countdown = new AsyncCountdownEvent(iterations * 10);
                await Run.InParallelAsync(10, i => {
                    return messageBus.SubscribeAsync<SimpleMessageA>(msg => {
                        Assert.Equal("Hello", msg.Data);
                        countdown.Signal();
                    });
                });

                await Run.InParallelAsync(iterations, i => messageBus.PublishAsync(new SimpleMessageA { Data = "Hello" }));
                await countdown.WaitAsync(TimeSpan.FromSeconds(2));
                Assert.NotEqual(0, countdown.CurrentCount);
            }
            finally {
                await CleanupMessageBusAsync(messageBus);
            }
        }

        [Fact]
        public override Task CanReceiveMessagesConcurrentlyAsync() {
            return base.CanReceiveMessagesConcurrentlyAsync();
        }

        [Fact]
        public override Task CanSendMessageToMultipleSubscribersAsync() {
            return base.CanSendMessageToMultipleSubscribersAsync();
        }

        [Fact]
        public override Task CanTolerateSubscriberFailureAsync() {
            return base.CanTolerateSubscriberFailureAsync();
        }

        [Fact]
        public override Task WillOnlyReceiveSubscribedMessageTypeAsync() {
            return base.WillOnlyReceiveSubscribedMessageTypeAsync();
        }

        [Fact]
        public override Task WillReceiveDerivedMessageTypesAsync() {
            return base.WillReceiveDerivedMessageTypesAsync();
        }

        [Fact]
        public override Task CanSubscribeToAllMessageTypesAsync() {
            return base.CanSubscribeToAllMessageTypesAsync();
        }

        [Fact]
        public override Task CanCancelSubscriptionAsync() {
            return base.CanCancelSubscriptionAsync();
        }

        [Fact]
        public override Task WontKeepMessagesWithNoSubscribersAsync() {
            return base.WontKeepMessagesWithNoSubscribersAsync();
        }

        [Fact]
        public override Task CanReceiveFromMultipleSubscribersAsync() {
            return base.CanReceiveFromMultipleSubscribersAsync();
        }

        [Fact]
        public override void CanDisposeWithNoSubscribersOrPublishers() {
            base.CanDisposeWithNoSubscribersOrPublishers();
        }


    }
}