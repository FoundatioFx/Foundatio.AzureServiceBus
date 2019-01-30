﻿using System;
using Microsoft.Azure.ServiceBus;

namespace Foundatio.Messaging {
    public class AzureServiceBusMessageBusOptions : SharedMessageBusOptions {
        public string ClientId { get; set; }

        public string ClientSecret { get; set; }

        public string TenantId { get; set; }

        public string SubscriptionId { get; set; }

        public string ResourceGroupName { get; set; }

        public string NameSpaceName { get; set; }

        public string ConnectionString { get; set; }

        /// <summary>
        /// Prefetching enables the queue or subscription client to load additional messages from the service when it performs a receive operation.
        /// https://docs.microsoft.com/en-us/azure/service-bus-messaging/service-bus-performance-improvements
        /// </summary>
        public int? PrefetchCount { get; set; }

        /// <summary>
        /// Gets or sets the maximum number of concurrent calls to the callback the message pump should initiate. Default value is 1
        /// </summary>
        public int? MaxConcurrentCalls { get; set; }

        /// <summary>
        /// The idle interval after which the topic is automatically deleted. The minimum duration is 5 minutes.
        /// </summary>
        public TimeSpan? TopicAutoDeleteOnIdle { get; set; }

        /// <summary>
        /// The default message time to live value for a topic
        /// </summary>
        public TimeSpan? TopicDefaultMessageTimeToLive { get; set; }

        /// <summary>
        /// The maximum size of the topic in megabytes.
        /// </summary>
        public long? TopicMaxSizeInMegabytes { get; set; }

        /// <summary>
        /// Set to true if topic requires duplicate detection.
        /// </summary>
        public bool? TopicRequiresDuplicateDetection { get; set; }

        /// <summary>
        /// The duration of the duplicate detection history.
        /// </summary>
        public TimeSpan? TopicDuplicateDetectionHistoryTimeWindow { get; set; }

        /// <summary>
        /// Returns true if server-side batched operations are enabled.
        /// </summary>
        public bool? TopicEnableBatchedOperations { get; set; }

        /// <summary>
        /// Controls whether messages should be filtered before publishing.
        /// </summary>
        public bool? TopicEnableFilteringMessagesBeforePublishing { get; set; }

        /// <summary>
        /// Returns true if the message is anonymous accessible.
        /// </summary>
        public bool? TopicIsAnonymousAccessible { get; set; }

        /// <summary>
        /// Gets or sets a value that indicates whether the message-pump should call Complete(Guid) or Complete(Guid) on messages
        /// after the callback has completed processing. By default its set to TRUE
        /// </summary>
        public bool? AutoComplete { get; set; }

        /// <summary>
        /// There are wo different receive modes in Service Bus. PeekLock is set by default. For Subscription its best to use 
        /// ReceiveAndDelete with AutoComplete set to true or PeekAndLock with AutoComplete set to true.
        /// This way azure message pump will take care of calling completeasync for you.
        /// </summary>
        public ReceiveMode ReceiveMode { get; set; }

        /// <summary>
        /// Returns the status of the topic (enabled or disabled). When an entity is disabled, that entity cannot send or receive messages.
        /// </summary>
        public EntityStatus? TopicStatus { get; set; }

        /// <summary>
        /// Returns true if the queue supports ordering.
        /// </summary>
        public bool? TopicSupportOrdering { get; set; }

        /// <summary>
        /// Returns true if the topic is to be partitioned across multiple message brokers.
        /// </summary>
        public bool? TopicEnablePartitioning { get; set; }

        /// <summary>
        /// Returns true if the queue holds a message in memory temporarily before writing it to persistent storage.
        /// </summary>
        public bool? TopicEnableExpress { get; set; }

        /// <summary>
        /// Returns user metadata.
        /// </summary>
        public string TopicUserMetadata { get; set; }

        /// <summary>
        /// If no subscription name is specified, then a fanout type message bus will be created.
        /// </summary>
        public string SubscriptionName { get; set; }

        /// <summary>
        /// The idle interval after which the subscription is automatically deleted. The minimum duration is 5 minutes.
        /// </summary>
        public TimeSpan? SubscriptionAutoDeleteOnIdle { get; set; }

        /// <summary>
        /// The default message time to live.
        /// </summary>
        public TimeSpan? SubscriptionDefaultMessageTimeToLive { get; set; }

        /// <summary>
        /// The lock duration time span for the subscription.
        /// </summary>
        public TimeSpan? SubscriptionWorkItemTimeout { get; set; }

        /// <summary>
        /// the value indicating if a subscription supports the concept of session.
        /// </summary>
        public bool? SubscriptionRequiresSession { get; set; }

        /// <summary>
        /// Returns true if the subscription has dead letter support when a message expires.
        /// </summary>
        public bool? SubscriptionEnableDeadLetteringOnMessageExpiration { get; set; }

        /// <summary>
        /// Returns true if the subscription has dead letter support on filter evaluation exceptions.
        /// </summary>
        public bool? SubscriptionEnableDeadLetteringOnFilterEvaluationExceptions { get; set; }

        /// <summary>
        /// The number of maximum deliveries.
        /// </summary>
        public int? SubscriptionMaxDeliveryCount { get; set; }

        /// <summary>
        /// Returns true if server-side batched operations are enabled.
        /// </summary>
        public bool? SubscriptionEnableBatchedOperations { get; set; }

        /// <summary>
        /// Returns the status of the subcription (enabled or disabled). When an entity is disabled, that entity cannot send or receive messages.
        /// </summary>
        public EntityStatus? SubscriptionStatus { get; set; }

        /// <summary>
        /// Returns the path to the recipient to which the message is forwarded.
        /// </summary>
        public string SubscriptionForwardTo { get; set; }

        /// <summary>
        /// Returns the path to the recipient to which the dead lettered message is forwarded.
        /// </summary>
        public string SubscriptionForwardDeadLetteredMessagesTo { get; set; }

        /// <summary>
        /// Returns user metadata.
        /// </summary>
        public string SubscriptionUserMetadata { get; set; }

        public RetryPolicy SubscriptionRetryPolicy { get; set; }
    }

    public class AzureServiceBusMessageBusOptionsBuilder : SharedMessageBusOptionsBuilder<
        AzureServiceBusMessageBusOptions, AzureServiceBusMessageBusOptionsBuilder> {
        public AzureServiceBusMessageBusOptionsBuilder PrefetchCount(int prefetchCount) {
            Target.PrefetchCount = prefetchCount;
            return this;
        }

        public AzureServiceBusMessageBusOptionsBuilder TopicAutoDeleteOnIdle(TimeSpan topicAutoDeleteOnIdle) {
            Target.TopicAutoDeleteOnIdle = topicAutoDeleteOnIdle;
            return this;
        }

        public AzureServiceBusMessageBusOptionsBuilder TopicDefaultMessageTimeToLive(TimeSpan topicDefaultMessageTimeToLive) {
            Target.TopicDefaultMessageTimeToLive = topicDefaultMessageTimeToLive;
            return this;
        }

        public AzureServiceBusMessageBusOptionsBuilder TopicMaxSizeInMegabytes(long topicMaxSizeInMegabytes) {
            Target.TopicMaxSizeInMegabytes = topicMaxSizeInMegabytes;
            return this;
        }

        public AzureServiceBusMessageBusOptionsBuilder TopicRequiresDuplicateDetection(bool topicRequiresDuplicateDetection) {
            Target.TopicRequiresDuplicateDetection = topicRequiresDuplicateDetection;
            return this;
        }

        public AzureServiceBusMessageBusOptionsBuilder TopicDuplicateDetectionHistoryTimeWindow(TimeSpan topicDuplicateDetectionHistoryTimeWindow) {
            Target.TopicDuplicateDetectionHistoryTimeWindow = topicDuplicateDetectionHistoryTimeWindow;
            return this;
        }

        public AzureServiceBusMessageBusOptionsBuilder TopicEnableBatchedOperations(bool topicEnableBatchedOperations) {
            Target.TopicEnableBatchedOperations = topicEnableBatchedOperations;
            return this;
        }

        public AzureServiceBusMessageBusOptionsBuilder TopicEnableFilteringMessagesBeforePublishing(bool topicEnableFilteringMessagesBeforePublishing) {
            Target.TopicEnableFilteringMessagesBeforePublishing = topicEnableFilteringMessagesBeforePublishing;
            return this;
        }

        public AzureServiceBusMessageBusOptionsBuilder TopicIsAnonymousAccessible(bool topicIsAnonymousAccessible) {
            Target.TopicIsAnonymousAccessible = topicIsAnonymousAccessible;
            return this;
        }

        public AzureServiceBusMessageBusOptionsBuilder TopicStatus(EntityStatus topicStatus) {
            Target.TopicStatus = topicStatus;
            return this;
        }

        public AzureServiceBusMessageBusOptionsBuilder TopicSupportOrdering(bool topicSupportOrdering) {
            Target.TopicSupportOrdering = topicSupportOrdering;
            return this;
        }

        public AzureServiceBusMessageBusOptionsBuilder TopicEnablePartitioning(bool topicEnablePartitioning) {
            Target.TopicEnablePartitioning = topicEnablePartitioning;
            return this;
        }

        public AzureServiceBusMessageBusOptionsBuilder TopicEnableExpress(bool topicEnableExpress) {
            Target.TopicEnableExpress = topicEnableExpress;
            return this;
        }

        public AzureServiceBusMessageBusOptionsBuilder TopicUserMetadata(string topicUserMetadata) {
            Target.TopicUserMetadata = topicUserMetadata ?? throw new ArgumentNullException(nameof(topicUserMetadata));
            return this;
        }

        public AzureServiceBusMessageBusOptionsBuilder SubscriptionName(string subscriptionName) {
            Target.SubscriptionName = subscriptionName ?? throw new ArgumentNullException(nameof(subscriptionName));
            return this;
        }

        public AzureServiceBusMessageBusOptionsBuilder SubscriptionAutoDeleteOnIdle(TimeSpan subscriptionAutoDeleteOnIdle) {
            Target.SubscriptionAutoDeleteOnIdle = subscriptionAutoDeleteOnIdle;
            return this;
        }

        public AzureServiceBusMessageBusOptionsBuilder SubscriptionDefaultMessageTimeToLive(TimeSpan subscriptionDefaultMessageTimeToLive) {
            Target.SubscriptionDefaultMessageTimeToLive = subscriptionDefaultMessageTimeToLive;
            return this;
        }

        public AzureServiceBusMessageBusOptionsBuilder SubscriptionWorkItemTimeout(TimeSpan subscriptionWorkItemTimeout) {
            Target.SubscriptionWorkItemTimeout = subscriptionWorkItemTimeout;
            return this;
        }

        public AzureServiceBusMessageBusOptionsBuilder SubscriptionRequiresSession(bool subscriptionRequiresSession) {
            Target.SubscriptionRequiresSession = subscriptionRequiresSession;
            return this;
        }

        public AzureServiceBusMessageBusOptionsBuilder SubscriptionEnableDeadLetteringOnMessageExpiration(bool subscriptionEnableDeadLetteringOnMessageExpiration) {
            Target.SubscriptionEnableDeadLetteringOnMessageExpiration = subscriptionEnableDeadLetteringOnMessageExpiration;
            return this;
        }

        public AzureServiceBusMessageBusOptionsBuilder SubscriptionEnableDeadLetteringOnFilterEvaluationExceptions(bool subscriptionEnableDeadLetteringOnFilterEvaluationExceptions) {
            Target.SubscriptionEnableDeadLetteringOnFilterEvaluationExceptions = subscriptionEnableDeadLetteringOnFilterEvaluationExceptions;
            return this;
        }

        public AzureServiceBusMessageBusOptionsBuilder SubscriptionMaxDeliveryCount(int subscriptionMaxDeliveryCount) {
            Target.SubscriptionMaxDeliveryCount = subscriptionMaxDeliveryCount;
            return this;
        }

        public AzureServiceBusMessageBusOptionsBuilder SubscriptionEnableBatchedOperations(bool subscriptionEnableBatchedOperations) {
            Target.SubscriptionEnableBatchedOperations = subscriptionEnableBatchedOperations;
            return this;
        }

        public AzureServiceBusMessageBusOptionsBuilder SubscriptionStatus(EntityStatus subscriptionStatus) {
            Target.SubscriptionStatus = subscriptionStatus;
            return this;
        }

        public AzureServiceBusMessageBusOptionsBuilder SubscriptionForwardTo(string subscriptionForwardTo) {
            Target.SubscriptionForwardTo = subscriptionForwardTo ?? throw new ArgumentNullException(nameof(subscriptionForwardTo));
            return this;
        }

        public AzureServiceBusMessageBusOptionsBuilder SubscriptionForwardDeadLetteredMessagesTo(string subscriptionForwardDeadLetteredMessagesTo) {
            Target.SubscriptionForwardDeadLetteredMessagesTo = subscriptionForwardDeadLetteredMessagesTo ?? throw new ArgumentNullException(nameof(subscriptionForwardDeadLetteredMessagesTo));
            return this;
        }

        public AzureServiceBusMessageBusOptionsBuilder SubscriptionUserMetadata(string subscriptionUserMetadata) {
            Target.SubscriptionUserMetadata = subscriptionUserMetadata ?? throw new ArgumentNullException(nameof(subscriptionUserMetadata));
            return this;
        }

        public AzureServiceBusMessageBusOptionsBuilder SubscriptionRetryPolicy(RetryPolicy subscriptionRetryPolicy) {
            Target.SubscriptionRetryPolicy = subscriptionRetryPolicy ?? throw new ArgumentNullException(nameof(subscriptionRetryPolicy));
            return this;
        }
    }
}