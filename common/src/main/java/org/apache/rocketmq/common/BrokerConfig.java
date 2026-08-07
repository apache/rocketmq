/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.common;

import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.common.annotation.ImportantField;
import org.apache.rocketmq.common.config.ConfigManagerVersion;
import org.apache.rocketmq.common.constant.PermName;
import org.apache.rocketmq.common.message.MessageRequestMode;
import org.apache.rocketmq.common.metrics.MetricsExporterType;
import org.apache.rocketmq.common.topic.TopicValidator;
import org.apache.rocketmq.common.utils.NetworkUtil;

public class BrokerConfig extends BrokerIdentity {

    // ==================== Network/Identity ====================

    // Path to the broker configuration file.
    private String brokerConfigPath = null;

    // Root directory for RocketMQ installation.
    private String rocketmqHome = MixAll.ROCKETMQ_HOME_DIR;
    @ImportantField
    // Address of the NameServer(s), comma-separated.
    private String namesrvAddr = System.getProperty(MixAll.NAMESRV_ADDR_PROPERTY, System.getenv(MixAll.NAMESRV_ADDR_ENV));

    /**
     * Listen port for single broker
     */
    @ImportantField
    private int listenPort = 6888;

    @ImportantField
    // Primary IP address for the broker.
    private String brokerIP1 = NetworkUtil.getLocalAddress();
    // Secondary IP address for the broker (used for HA/dual NIC).
    private String brokerIP2 = NetworkUtil.getLocalAddress();

    @ImportantField
    // Whether to recover message store concurrently on startup.
    private boolean recoverConcurrently = false;

    // ==================== Topic/Subscription ====================

    // Permission for this broker (read/write flags).
    private int brokerPermission = PermName.PERM_READ | PermName.PERM_WRITE;
    // Default number of queues for auto-created topics.
    private int defaultTopicQueueNums = 8;
    @ImportantField
    // Whether to automatically create topics when sending to a non-existent topic.
    private boolean autoCreateTopicEnable = true;

    // Whether to enable cluster-level topic registration.
    private boolean clusterTopicEnable = true;

    // Whether to enable broker-level topic registration.
    private boolean brokerTopicEnable = true;
    @ImportantField
    // Whether to automatically create subscription groups when consuming from a non-existent group.
    private boolean autoCreateSubscriptionGroup = true;
    // Message store plugin class name for custom storage logic.
    private String messageStorePlugIn = "";

    // Number of available processors (used for thread pool sizing).
    private static final int PROCESSOR_NUMBER = Runtime.getRuntime().availableProcessors();
    @ImportantField
    // Topic name for message tracing.
    private String msgTraceTopicName = TopicValidator.RMQ_SYS_TRACE_TOPIC;
    @ImportantField
    // Whether to enable message tracing topic.
    private boolean traceTopicEnable = false;
    // ==================== Thread Pool Sizes ====================

    /**
     * thread numbers for send message thread pool.
     */
    private int sendMessageThreadPoolNums = Math.min(PROCESSOR_NUMBER, 4);
    // Thread pool size for put message future handling.
    private int putMessageFutureThreadPoolNums = Math.min(PROCESSOR_NUMBER, 4);
    // Thread pool size for pull message processing.
    private int pullMessageThreadPoolNums = 16 + PROCESSOR_NUMBER * 2;
    // Thread pool size for lite pull message processing.
    private int litePullMessageThreadPoolNums = 16 + PROCESSOR_NUMBER * 2;
    // Thread pool size for ack message processing.
    private int ackMessageThreadPoolNums = 16;
    // Thread pool size for reply message processing.
    private int processReplyMessageThreadPoolNums = 16 + PROCESSOR_NUMBER * 2;
    // Thread pool size for query message processing.
    private int queryMessageThreadPoolNums = 8 + PROCESSOR_NUMBER;

    // Thread pool size for admin broker operations.
    private int adminBrokerThreadPoolNums = 16;
    // Thread pool size for client management.
    private int clientManageThreadPoolNums = 32;
    // Thread pool size for consumer management.
    private int consumerManageThreadPoolNums = 32;
    // Thread pool size for load balance processing.
    private int loadBalanceProcessorThreadPoolNums = 32;
    // Thread pool size for heartbeat processing.
    private int heartbeatThreadPoolNums = Math.min(32, PROCESSOR_NUMBER);
    // Thread pool size for store recovery.
    private int recoverThreadPoolNums = 32;

    /**
     * Thread numbers for EndTransactionProcessor
     */
    private int endTransactionThreadPoolNums = Math.max(8 + PROCESSOR_NUMBER * 2,
            sendMessageThreadPoolNums * 4);

    // Interval (ms) to persist consumer offsets to disk (default 5s)
    private int flushConsumerOffsetInterval = 1000 * 5;

    // Interval (ms) to record consumer offset persistence history (default 60s)
    private int flushConsumerOffsetHistoryInterval = 1000 * 60;

    // Whether to reject transaction messages (disable transaction message feature)
    @ImportantField
    private boolean rejectTransactionMessage = false;

    // Whether to resolve NameServer address via DNS lookup
    @ImportantField
    private boolean fetchNameSrvAddrByDnsLookup = false;

    // Whether to fetch NameServer address from an address server (HTTP endpoint)
    @ImportantField
    private boolean fetchNamesrvAddrByAddressServer = false;

    // ==================== Thread Pool Queue Capacities ====================

    // Queue capacity for send message thread pool.
    private int sendThreadPoolQueueCapacity = 10000;
    // Queue capacity for put message thread pool.
    private int putThreadPoolQueueCapacity = 10000;
    // Queue capacity for pull message thread pool.
    private int pullThreadPoolQueueCapacity = 100000;
    // Queue capacity for lite pull message thread pool.
    private int litePullThreadPoolQueueCapacity = 100000;
    // Queue capacity for ack message thread pool.
    private int ackThreadPoolQueueCapacity = 100000;
    // Queue capacity for reply message thread pool.
    private int replyThreadPoolQueueCapacity = 10000;
    // Queue capacity for query message thread pool.
    private int queryThreadPoolQueueCapacity = 20000;
    // Queue capacity for client manager thread pool.
    private int clientManagerThreadPoolQueueCapacity = 1000000;
    // Queue capacity for consumer manager thread pool.
    private int consumerManagerThreadPoolQueueCapacity = 1000000;
    // Queue capacity for heartbeat thread pool.
    private int heartbeatThreadPoolQueueCapacity = 50000;
    // Queue capacity for end transaction thread pool.
    private int endTransactionPoolQueueCapacity = 100000;
    // Queue capacity for admin broker thread pool.
    private int adminBrokerThreadPoolQueueCapacity = 10000;
    // Queue capacity for load balance thread pool.
    private int loadBalanceThreadPoolQueueCapacity = 100000;

    // ==================== Polling/Notification ====================

    // Whether to enable long polling for message delivery.
    private boolean longPollingEnable = true;

    // Maximum time in milliseconds for short polling.
    private long shortPollingTimeMills = 1000;

    // Whether to notify when consumer IDs change.
    private boolean notifyConsumerIdsChangedEnable = true;

    // ==================== Commercial/Stats ====================

    // Whether to enable high-speed mode for commercial usage.
    private boolean highSpeedMode = false;

    // Base count for commercial billing.
    private int commercialBaseCount = 1;

    // Message size unit for commercial billing.
    private int commercialSizePerMsg = 4 * 1024;

    // Whether to enable account statistics collection.
    private boolean accountStatsEnable = true;
    // Whether to print zero values in account statistics.
    private boolean accountStatsPrintZeroValues = true;

    // Maximum idle time in minutes before stats are considered stale (-1 for unlimited).
    private int maxStatsIdleTimeInMinutes = -1;

    // ==================== Transfer/Region ====================

    // Whether to transfer messages via heap memory.
    private boolean transferMsgByHeap = true;

    // Region ID for this broker.
    private String regionId = MixAll.DEFAULT_TRACE_REGION_ID;
    // Timeout in milliseconds for broker registration.
    private int registerBrokerTimeoutMills = 24000;

    // Timeout in milliseconds for sending heartbeat.
    private int sendHeartbeatTimeoutMillis = 1000;

    // ==================== Slave/SlowConsumer ====================

    // Whether to allow slave brokers to read messages.
    private boolean slaveReadEnable = false;

    // Whether to disable consumption if consumer is reading slowly.
    private boolean disableConsumeIfConsumerReadSlowly = false;
    // Threshold in bytes for determining if a consumer is falling behind.
    private long consumerFallbehindThreshold = 1024L * 1024 * 1024 * 16;

    // ==================== FastFailure ====================

    // Whether to enable fast failure for broker requests.
    private boolean brokerFastFailureEnable = true;
    // Max wait time in send queue before fast failure.
    private long waitTimeMillsInSendQueue = 200;
    // Max wait time in pull queue before fast failure.
    private long waitTimeMillsInPullQueue = 5 * 1000;
    // Max wait time in lite pull queue before fast failure.
    private long waitTimeMillsInLitePullQueue = 5 * 1000;
    // Max wait time in heartbeat queue before fast failure.
    private long waitTimeMillsInHeartbeatQueue = 31 * 1000;
    // Max wait time in transaction queue before fast failure.
    private long waitTimeMillsInTransactionQueue = 3 * 1000;
    // Max wait time in ack queue before fast failure.
    private long waitTimeMillsInAckQueue = 3000;
    // Max wait time in admin broker queue before fast failure.
    private long waitTimeMillsInAdminBrokerQueue = 5 * 1000;
    // Timestamp when broker starts accepting send requests.
    private long startAcceptSendRequestTimeStamp = 0L;

    // Whether to enable message tracing.
    private boolean traceOn = true;

    // Switch of filter bit map calculation.
    // If switch on:
    // 1. Calculate filter bit map when construct queue.
    // 2. Filter bit map will be saved to consume queue extend file if allowed.
    private boolean enableCalcFilterBitMap = false;

    //Reject the pull consumer instance to pull messages from broker.
    private boolean rejectPullConsumerEnable = false;

    // Expect num of consumers will use filter.
    private int expectConsumerNumUseFilter = 32;

    // Error rate of bloom filter, 1~100.
    private int maxErrorRateOfBloomFilter = 20;

    //how long to clean filter data after dead.Default: 24h
    private long filterDataCleanTimeSpan = 24 * 3600 * 1000;

    // whether do filter when retry.
    private boolean filterSupportRetry = false;
    // Whether to enable property-based message filtering.
    private boolean enablePropertyFilter = false;

    // ==================== Register/NameServer ====================

    // Whether to use compressed registration data.
    private boolean compressedRegister = false;

    // Whether to force registration even if no changes detected.
    private boolean forceRegister = true;

    /**
     * This configurable item defines interval of topics registration of broker to name server. Allowing values are
     * between 10,000 and 60,000 milliseconds.
     */
    private int registerNameServerPeriod = 1000 * 30;

    /**
     * This configurable item defines interval of update name server address. Default: 120 * 1000 milliseconds
     */
    private int updateNameServerAddrPeriod = 1000 * 120;

    /**
     * the interval to send heartbeat to name server for liveness detection.
     */
    private int brokerHeartbeatInterval = 1000;

    /**
     * How long the broker will be considered as inactive by nameserver since last heartbeat. Effective only if
     * enableSlaveActingMaster is true
     */
    private long brokerNotActiveTimeoutMillis = 10 * 1000;

    // ==================== FlowControl/Broadcast ====================

    // Whether to enable network flow control.
    private boolean enableNetWorkFlowControl = false;

    // Whether to enable broadcast offset storage.
    private boolean enableBroadcastOffsetStore = true;

    // Expiration time in seconds for broadcast offsets.
    private long broadcastOffsetExpireSecond = 2 * 60;

    // Maximum expiration time in seconds for broadcast offsets.
    private long broadcastOffsetExpireMaxSecond = 5 * 60;

    // ==================== Pop Core ====================

    // Number of messages to poll in one pop request.
    private int popPollingSize = 1024;
    // Size of the pop polling map.
    private int popPollingMapSize = 100000;

    // Expiration time in seconds for pop polling map entries.
    private int popPollingMapExpireTimeSeconds = 60 * 10;
    // 20w cost 200M heap memory.
    private long maxPopPollingSize = 100000;
    // Number of revive queues for pop consumption.
    private int reviveQueueNum = 8;
    // Interval in milliseconds for revive scanning.
    private long reviveInterval = 1000;
    // Maximum slow times before revive throttling.
    private long reviveMaxSlow = 3;
    // Scan time in milliseconds for revive processing.
    private long reviveScanTime = 10000;
    // Whether to skip long awaiting ack during revive.
    private boolean enableSkipLongAwaitingAck = false;
    // Wait time in milliseconds for revive ack.
    private long reviveAckWaitMs = TimeUnit.MINUTES.toMillis(3);
    // Whether to enable pop logging.
    private boolean enablePopLog = false;
    // Whether to enable pop buffer merge.
    private boolean enablePopBufferMerge = false;
    // Time in milliseconds for pop CK to stay in buffer.
    private int popCkStayBufferTime = 10 * 1000;
    // Timeout in milliseconds for pop CK buffer stay.
    private int popCkStayBufferTimeOut = 3 * 1000;
    // Max buffer size for pop CK.
    private int popCkMaxBufferSize = 200000;
    // Max queue size for pop CK offset.
    private int popCkOffsetMaxQueueSize = 20000;
    // Whether to enable batch ack for pop messages.
    private boolean enablePopBatchAck = false;
    // set the interval to the maxFilterMessageSize in MessageStoreConfig divided by the cq unit size
    private long popLongPollingForceNotifyInterval = 800;
    // Whether to calculate lag before pop notification.
    private boolean enableNotifyBeforePopCalculateLag = true;
    // Whether to notify after pop order lock release.
    private boolean enableNotifyAfterPopOrderLockRelease = true;
    // Whether to init pop offset by checking msg in memory.
    private boolean initPopOffsetByCheckMsgInMem = true;
    // read message from pop retry topic v1, for the compatibility, will be removed in the future version
    private boolean retrieveMessageFromPopRetryTopicV1 = true;

    // ==================== Pop Retry ====================

    // Whether to enable retry topic v2 format.
    private boolean enableRetryTopicV2 = false;
    // Probability percentage for popping from retry topic.
    private int popFromRetryProbability = 20;
    // pop retry probability for priority mode
    private int popFromRetryProbabilityForPriority = 0;
    // 0 as the lowest priority if true
    private boolean priorityOrderAsc = true;

    // ==================== Pop Ack ====================

    /**
     * There are two types of ack mode:
     *  1. ack by file system service, which is the default mode.
     *  2. ack by key-value service, when popConsumerKVServiceEnable and popConsumerKVServiceInit are both true.
     */
    private boolean popConsumerFSServiceInit = true;
    // Whether to enable logging for pop consumer KV service.
    private boolean popConsumerKVServiceLog = false;
    // Whether to initialize pop consumer KV service.
    private boolean popConsumerKVServiceInit = false;
    // Whether to enable pop consumer KV service.
    private boolean popConsumerKVServiceEnable = false;

    // ==================== Pop Revive ====================

    // Max return size per read for pop revive.
    private int popReviveMaxReturnSizePerRead = 16 * 1024;
    // Concurrency level for pop revive processing.
    private int popReviveConcurrency = 32;
    // Max attempt times for pop revive.
    private int popReviveMaxAttemptTimes = 16;
    // Whether to skip revive if consumer group is absent.
    private boolean popReviveSkipIfGroupAbsent = true;
    // each message queue will have a corresponding retry queue
    private boolean useSeparateRetryQueue = false;
    // Whether to notify consumer change in real time.
    private boolean realTimeNotifyConsumerChange = true;

    // ==================== Notification/Filter ====================

    // Whether to use message filter for notifications.
    private boolean useMessageFilterForNotification = true;
    // Max number of message filters for notifications.
    private int maxMessageFilterNumForNotification = 64;

    // ==================== LitePull ====================

    // Whether to enable lite pull message mode.
    private boolean litePullMessageEnable = true;

    // ==================== LoadBalance ====================

    // The period to sync broker member group from namesrv, default value is 1 second
    private int syncBrokerMemberGroupPeriod = 1000;

    /**
     * the interval of pulling topic information from the named server
     */
    private long loadBalancePollNameServerInterval = 1000 * 30;

    /**
     * the interval of cleaning
     */
    private int cleanOfflineBrokerInterval = 1000 * 30;

    // Whether to enable server-side load balancer.
    private boolean serverLoadBalancerEnable = true;

    // Default message request mode (PULL or POP).
    private MessageRequestMode defaultMessageRequestMode = MessageRequestMode.PULL;

    // Default number of shared queues for pop mode (-1 means use topic queue num).
    private int defaultPopShareQueueNum = -1;

    // ==================== Transaction ====================

    /**
     * The minimum time of the transactional message  to be checked firstly, one message only exceed this time interval
     * that can be checked.
     */
    @ImportantField
    private long transactionTimeOut = 6 * 1000;

    /**
     * The maximum number of times the message was checked, if exceed this value, this message will be discarded.
     */
    @ImportantField
    private int transactionCheckMax = 15;

    /**
     * Transaction message check interval.
     */
    @ImportantField
    private long transactionCheckInterval = 30 * 1000;

    // Flush interval for transaction metrics.
    private long transactionMetricFlushInterval = 10 * 1000;

    // Core thread count for transaction check RocksDB.
    private int transactionCheckRocksdbCoreThreads = 2;

    // Max thread count for transaction check RocksDB.
    private int transactionCheckRocksdbMaxThreads = 5;

    // Queue capacity for transaction check RocksDB.
    private int transactionCheckRocksdbQueueCapacity = 2000;

    /**
     * transaction batch op message
     */
    private int transactionOpMsgMaxSize = 4096;

    // Batch interval for transaction op messages.
    private int transactionOpBatchInterval = 3000;

    // ==================== ACL ====================

    /**
     * Acl feature switch
     */
    @ImportantField
    private boolean aclEnable = false;

    // ==================== Reply/Stats ====================

    // Whether to enable storing reply messages.
    private boolean storeReplyMessageEnable = true;

    // Whether to enable detailed statistics.
    private boolean enableDetailStat = true;

    // Whether to auto-delete unused stats.
    private boolean autoDeleteUnusedStats = true;

    /**
     * Whether to distinguish log paths when multiple brokers are deployed on the same machine
     */
    private boolean isolateLogEnable = false;

    // Timeout in milliseconds for message forwarding.
    private long forwardTimeout = 3 * 1000;

    // ==================== Failover ====================

    /**
     * Slave will act master when failover. For example, if master down, timer or transaction message which is expire in slave will
     * put to master (master of the same process in broker container mode or other masters in cluster when enableFailoverRemotingActing is true)
     * when enableSlaveActingMaster is true
     */
    private boolean enableSlaveActingMaster = false;

    // Whether to enable remote escape for failover.
    private boolean enableRemoteEscape = false;

    // Whether to skip pre-online checks.
    private boolean skipPreOnline = false;

    // ==================== Async/Offset ====================

    // Whether to enable async send mode.
    private boolean asyncSendEnable = true;

    // Whether to use server-side reset offset.
    private boolean useServerSideResetOffset = true;

    // Step size for consumer offset update version.
    private long consumerOffsetUpdateVersionStep = 500;

    // Step size for delay offset update version.
    private long delayOffsetUpdateVersionStep = 200;

    // ==================== HA/Lock ====================

    /**
     * Whether to lock quorum replicas.
     *
     * True: need to lock quorum replicas succeed. False: only need to lock one replica succeed.
     */
    private boolean lockInStrictMode = false;

    // Whether to be compatible with old NameServer protocol.
    private boolean compatibleWithOldNameSrv = true;

    // ==================== Controller ====================

    /**
     * Is startup controller mode, which support auto switch broker's role.
     */
    private boolean enableControllerMode = false;

    // Address of the controller.
    private String controllerAddr = "";

    // Whether to fetch controller address via DNS lookup.
    private boolean fetchControllerAddrByDnsLookup = false;

    // Period in milliseconds for syncing broker metadata.
    private long syncBrokerMetadataPeriod = 5 * 1000;

    // Period in milliseconds for checking sync state set.
    private long checkSyncStateSetPeriod = 5 * 1000;

    // Period in milliseconds for syncing controller metadata.
    private long syncControllerMetadataPeriod = 10 * 1000;

    // Timeout in milliseconds for controller heartbeat.
    private long controllerHeartBeatTimeoutMills = 10 * 1000;

    // ==================== Topic/Group Mgmt ====================

    // Whether to validate system topic when updating topic.
    private boolean validateSystemTopicWhenUpdateTopic = true;

    /**
     * Maximum rate (permits per second) for batch topic deletion.
     * Setting to 0 or negative disables rate limiting.
     */
    private double batchDeleteTopicMaxRate = 10.0;

    /**
     * Maximum rate (permits per second) for batch subscription-group deletion.
     * Setting to 0 or negative disables rate limiting.
     */
    private double batchDeleteSubscriptionGroupMaxRate = 10.0;

    /**
     * It is an important basis for the controller to choose the broker master.
     * The lower the value of brokerElectionPriority, the higher the priority of the broker being selected as the master.
     * You can set a lower priority for the broker with better machine conditions.
     */
    private int brokerElectionPriority = Integer.MAX_VALUE;

    // Whether to use static subscription configuration.
    private boolean useStaticSubscription = false;

    // ==================== Metrics ====================

    // Type of metrics exporter (DISABLE, OTLP_GRPC, PROM, etc.).
    private MetricsExporterType metricsExporterType = MetricsExporterType.DISABLE;

    // Cardinality limit for OpenTelemetry metrics.
    private int metricsOtelCardinalityLimit = 50 * 1000;
    // Target URL for gRPC metrics exporter.
    private String metricsGrpcExporterTarget = "";
    // Headers for gRPC metrics exporter.
    private String metricsGrpcExporterHeader = "";
    // Timeout in milliseconds for gRPC metrics export.
    private long metricGrpcExporterTimeOutInMills = 3 * 1000;
    // Interval in milliseconds for gRPC metrics export.
    private long metricGrpcExporterIntervalInMills = 60 * 1000;
    // Interval in milliseconds for logging metrics export.
    private long metricLoggingExporterIntervalInMills = 10 * 1000;

    // Port for Prometheus metrics exporter.
    private int metricsPromExporterPort = 5557;
    // Host for Prometheus metrics exporter.
    private String metricsPromExporterHost = "";

    // Label pairs in CSV. Each label follows pattern of Key:Value. eg: instance_id:xxx,uid:xxx
    private String metricsLabel = "";

    /**
     * Whether to wrap {@code OtlpGrpcMetricExporter} with
     * {@code BatchSplittingMetricExporter}. When {@code true} (default)
     * the splitter is active and guards against oversized OTLP payloads
     * via sub-batching. Set to {@code false} to disable the wrapper
     * entirely (escape hatch: use the raw exporter when the splitter
     * itself is suspected of misbehaving, or when cardinality is known
     * to stay well below the server payload limit).
     */
    private boolean metricsExportBatchSplitEnabled = true;

    // Max data points per metrics export batch.
    private int metricsExportBatchMaxDataPoints = 1000;

    /**
     * Max in-flight sub-batches per export() cycle when the batch splitter
     * triggers. Bounds the MetricData retention window under OTel SDK
     * 1.31+ where the OTLP exporter may hold metric references until the
     * gRPC RPC completes. 0 or negative means unlimited (legacy behavior).
     * Only effective when {@code metricsExportBatchSplitEnabled} is true.
     */
    private int metricsExportBatchMaxConcurrent = 4;

    /**
     * Memory mode for OtlpGrpcMetricExporter. Valid values (case-insensitive):
     * "IMMUTABLE_DATA" (default, safe) or "REUSABLE_DATA".
     * <p>
     * OpenTelemetry Java 1.44.0 ~ 1.46.x ships REUSABLE_DATA as the default
     * but its MetricReusableDataMarshaler uses a non-thread-safe ArrayDeque
     * pool. Combined with concurrent sub-batch export it leaks marshalers
     * until OOM (fixed upstream in 1.47.0, see opentelemetry-java#7041).
     * IMMUTABLE_DATA bypasses that pool entirely. Switch back to
     * REUSABLE_DATA only when running on OTel SDK >= 1.47.
     * <p>
     * Invalid values fall back to IMMUTABLE_DATA with a WARN log.
     */
    private String metricsExportOtelMemoryMode = "IMMUTABLE_DATA";

    // Whether to export metrics in delta mode.
    private boolean metricsInDelta = false;

    // Whether to enable remoting metrics.
    private boolean enableRemotingMetrics = true;
    // Whether to enable message store metrics.
    private boolean enableMessageStoreMetrics = true;
    // Whether to enable pop metrics.
    private boolean enablePopMetrics = true;
    // Whether to enable connection metrics.
    private boolean enableConnectionMetrics = true;
    // Whether to enable transaction metrics.
    private boolean enableTransactionMetrics = true;
    // Whether to enable stats metrics.
    private boolean enableStatsMetrics = true;
    // Whether to enable request metrics.
    private boolean enableRequestMetrics = true;
    // Whether to enable lag and DLQ metrics.
    private boolean enableLagAndDlqMetrics = true;

    // ==================== Channel/Subscription ====================

    // Timeout in milliseconds for channel expiration.
    private long channelExpiredTimeout = 1000 * 120;
    // Timeout in milliseconds for subscription expiration.
    private long subscriptionExpiredTimeout = 1000 * 60 * 10;

    /**
     * Estimate accumulation or not when subscription filter type is tag and is not SUB_ALL.
     */
    private boolean estimateAccumulation = true;

    // ==================== ColdData ====================

    // Whether to enable cold data control strategy.
    private boolean coldCtrStrategyEnable = false;
    // Whether to use PID-based cold control strategy.
    private boolean usePIDColdCtrStrategy = true;
    // Threshold in bytes for cold group read.
    private long cgColdReadThreshold = 3 * 1024 * 1024;
    // Global threshold in bytes for cold read.
    private long globalColdReadThreshold = 100 * 1024 * 1024;

    // ==================== Misc ====================

    /**
     * The interval to fetch namesrv addr, default value is 10 second
     */
    private long fetchNamesrvAddrInterval = 10 * 1000;

    /**
     * Pop response returns the actual retry topic rather than tampering with the original topic
     */
    private boolean popResponseReturnActualRetryTopic = false;

    /**
     * If both the deleteTopicWithBrokerRegistration flag in the NameServer configuration and this flag are set to true,
     * it guarantees the ultimate consistency of data between the broker and the nameserver during topic deletion.
     */
    private boolean enableSingleTopicRegister = false;

    // Whether to enable mixed message type.
    private boolean enableMixedMessageType = false;

    /**
     * This flag and deleteTopicWithBrokerRegistration flag in the NameServer cannot be set to true at the same time,
     * otherwise there will be a loss of routing
     */
    private boolean enableSplitRegistration = false;

    // Whether to enable split metadata for registration.
    private boolean enableSplitMetadata = true;
    // Max size for split metadata batches.
    private int splitMetadataSize = 2000;

    // Threshold for pop inflight messages.
    private long popInflightMessageThreshold = 10000;
    // Whether to enable pop message threshold.
    private boolean enablePopMessageThreshold = false;

    // Whether to enable fast channel event processing.
    private boolean enableFastChannelEventProcess = false;
    // Whether to print channel groups for debugging.
    private boolean printChannelGroups = false;
    // Minimum number of channel groups to print.
    private int printChannelGroupsMinNum = 5;

    // Size for split registration batches.
    private int splitRegistrationSize = 800;

    /**
     * Config in this black list will be not allowed to update by command.
     * Try to update this config black list by restart process.
     * Try to update configures in black list by restart process.
     */
    private String configBlackList = "configBlackList;brokerConfigPath";

    // if false, will still rewrite ck after max times 17
    private boolean skipWhenCKRePutReachMaxTimes = false;

    // Whether to append ack asynchronously.
    private boolean appendAckAsync = false;

    // Whether to append CK asynchronously.
    private boolean appendCkAsync = false;

    // Whether to clear retry topic when deleting topic.
    private boolean clearRetryTopicWhenDeleteTopic = true;

    // Whether to enable LMQ stats.
    private boolean enableLmqStats = false;

    /**
     * V2 is recommended in cases where LMQ feature is extensively used.
     */
    private String configManagerVersion = ConfigManagerVersion.V1.getVersion();

    /**
     * Whether to use a single RocksDB instance with multiple column families for all configs
     * instead of separate RocksDB instances for Topic, Group, and Offset configs
     */
    private boolean useSingleRocksDBForAllConfigs = false;

    // Whether to allow recall when broker is not writeable.
    private boolean allowRecallWhenBrokerNotWriteable = true;

    // Whether to enable message recall.
    private boolean recallMessageEnable = false;

    // Whether to enable producer registration.
    private boolean enableRegisterProducer = true;

    // Whether to enable creation of system groups.
    private boolean enableCreateSysGroup = true;

    // ==================== LiteTopic ====================

    // Interval in milliseconds for lite event checking.
    private long liteEventCheckInterval = 10 * 1000;

    // Interval in milliseconds for lite TTL checking.
    private long liteTtlCheckInterval = 120 * 1000;

    // Minimum TTL in milliseconds for lite messages.
    private long minLiteTTl = 15 * 60 * 1000;

    // Interval for lite subscription checking.
    private long liteSubscriptionCheckInterval = TimeUnit.MINUTES.toMillis(2);

    // Timeout in milliseconds for lite subscription checking.
    private long liteSubscriptionCheckTimeoutMills = TimeUnit.MINUTES.toMillis(3);

    // make sense for rocksdb store
    private boolean persistConsumerOffsetIncrementally = false;

    // Max count for lite subscriptions.
    private long maxLiteSubscriptionCount = 100000;

    // Whether to enable lite pop logging.
    private boolean enableLitePopLog = false;

    // Max client event count before throttling.
    private int maxClientEventCount = 100;

    // TTL in milliseconds for lite event capacity cache.
    private long liteEventCapacityCacheTtlMs = 5000;

    // Delay time in milliseconds for lite event full dispatch.
    private long liteEventFullDispatchDelayTime = 10 * 1000;

    // Delay time in milliseconds for lite event full dispatch for wildcard groups.
    private long liteEventFullDispatchDelayTimeForWildcardGroup = 10 * 1000;

    // lite metrics
    // whether to collect storeTime in popLiteProcessor
    private boolean liteLagLatencyCollectEnable = false;

    // Whether to enable lite lag latency metrics.
    private boolean liteLagLatencyMetricsEnable = false;

    // Whether to enable lite lag count metrics.
    private boolean liteLagCountMetricsEnable = false;

    // Top K value for lite lag latency metrics.
    private int liteLagLatencyTopK = 50;

    // ==================== PopOrderLock ====================

    // HashedWheelTimer config for pop order lock manager
    private long popOrderLockTimerTickMs = 100;
    // Number of ticks per wheel for pop order lock timer.
    private int popOrderLockTimerTicksPerWheel = 512;

    public String getConfigBlackList() {
        return configBlackList;
    }

    public void setConfigBlackList(String configBlackList) {
        this.configBlackList = configBlackList;
    }

    public long getMaxPopPollingSize() {
        return maxPopPollingSize;
    }

    public void setMaxPopPollingSize(long maxPopPollingSize) {
        this.maxPopPollingSize = maxPopPollingSize;
    }

    public int getReviveQueueNum() {
        return reviveQueueNum;
    }

    public void setReviveQueueNum(int reviveQueueNum) {
        this.reviveQueueNum = reviveQueueNum;
    }

    public long getReviveInterval() {
        return reviveInterval;
    }

    public void setReviveInterval(long reviveInterval) {
        this.reviveInterval = reviveInterval;
    }

    public int getPopCkStayBufferTime() {
        return popCkStayBufferTime;
    }

    public void setPopCkStayBufferTime(int popCkStayBufferTime) {
        this.popCkStayBufferTime = popCkStayBufferTime;
    }

    public int getPopCkStayBufferTimeOut() {
        return popCkStayBufferTimeOut;
    }

    public void setPopCkStayBufferTimeOut(int popCkStayBufferTimeOut) {
        this.popCkStayBufferTimeOut = popCkStayBufferTimeOut;
    }

    public int getPopPollingMapSize() {
        return popPollingMapSize;
    }

    public void setPopPollingMapSize(int popPollingMapSize) {
        this.popPollingMapSize = popPollingMapSize;
    }

    public int getPopPollingMapExpireTimeSeconds() {
        return popPollingMapExpireTimeSeconds;
    }

    public void setPopPollingMapExpireTimeSeconds(int popPollingMapExpireTimeSeconds) {
        this.popPollingMapExpireTimeSeconds = popPollingMapExpireTimeSeconds;
    }

    public long getReviveScanTime() {
        return reviveScanTime;
    }

    public void setReviveScanTime(long reviveScanTime) {
        this.reviveScanTime = reviveScanTime;
    }

    public long getReviveMaxSlow() {
        return reviveMaxSlow;
    }

    public void setReviveMaxSlow(long reviveMaxSlow) {
        this.reviveMaxSlow = reviveMaxSlow;
    }

    public int getPopPollingSize() {
        return popPollingSize;
    }

    public void setPopPollingSize(int popPollingSize) {
        this.popPollingSize = popPollingSize;
    }

    public boolean isEnablePopBufferMerge() {
        return enablePopBufferMerge;
    }

    public void setEnablePopBufferMerge(boolean enablePopBufferMerge) {
        this.enablePopBufferMerge = enablePopBufferMerge;
    }

    public int getPopCkMaxBufferSize() {
        return popCkMaxBufferSize;
    }

    public void setPopCkMaxBufferSize(int popCkMaxBufferSize) {
        this.popCkMaxBufferSize = popCkMaxBufferSize;
    }

    public int getPopCkOffsetMaxQueueSize() {
        return popCkOffsetMaxQueueSize;
    }

    public void setPopCkOffsetMaxQueueSize(int popCkOffsetMaxQueueSize) {
        this.popCkOffsetMaxQueueSize = popCkOffsetMaxQueueSize;
    }

    public boolean isEnablePopBatchAck() {
        return enablePopBatchAck;
    }

    public void setEnablePopBatchAck(boolean enablePopBatchAck) {
        this.enablePopBatchAck = enablePopBatchAck;
    }

    public boolean isEnableSkipLongAwaitingAck() {
        return enableSkipLongAwaitingAck;
    }

    public void setEnableSkipLongAwaitingAck(boolean enableSkipLongAwaitingAck) {
        this.enableSkipLongAwaitingAck = enableSkipLongAwaitingAck;
    }

    public long getReviveAckWaitMs() {
        return reviveAckWaitMs;
    }

    public void setReviveAckWaitMs(long reviveAckWaitMs) {
        this.reviveAckWaitMs = reviveAckWaitMs;
    }

    public boolean isEnablePopLog() {
        return enablePopLog;
    }

    public void setEnablePopLog(boolean enablePopLog) {
        this.enablePopLog = enablePopLog;
    }

    public int getPopFromRetryProbability() {
        return popFromRetryProbability;
    }

    public void setPopFromRetryProbability(int popFromRetryProbability) {
        this.popFromRetryProbability = popFromRetryProbability;
    }

    public boolean isPopConsumerFSServiceInit() {
        return popConsumerFSServiceInit;
    }

    public void setPopConsumerFSServiceInit(boolean popConsumerFSServiceInit) {
        this.popConsumerFSServiceInit = popConsumerFSServiceInit;
    }

    public boolean isPopConsumerKVServiceLog() {
        return popConsumerKVServiceLog;
    }

    public void setPopConsumerKVServiceLog(boolean popConsumerKVServiceLog) {
        this.popConsumerKVServiceLog = popConsumerKVServiceLog;
    }

    public boolean isPopConsumerKVServiceInit() {
        return popConsumerKVServiceInit;
    }

    public void setPopConsumerKVServiceInit(boolean popConsumerKVServiceInit) {
        this.popConsumerKVServiceInit = popConsumerKVServiceInit;
    }

    public boolean isPopConsumerKVServiceEnable() {
        return popConsumerKVServiceEnable;
    }

    public void setPopConsumerKVServiceEnable(boolean popConsumerKVServiceEnable) {
        this.popConsumerKVServiceEnable = popConsumerKVServiceEnable;
    }

    public int getPopReviveConcurrency() {
        return popReviveConcurrency;
    }

    public void setPopReviveConcurrency(int popReviveConcurrency) {
        this.popReviveConcurrency = popReviveConcurrency;
    }

    public int getPopReviveMaxReturnSizePerRead() {
        return popReviveMaxReturnSizePerRead;
    }

    public void setPopReviveMaxReturnSizePerRead(int popReviveMaxReturnSizePerRead) {
        this.popReviveMaxReturnSizePerRead = popReviveMaxReturnSizePerRead;
    }

    public int getPopReviveMaxAttemptTimes() {
        return popReviveMaxAttemptTimes;
    }

    public void setPopReviveMaxAttemptTimes(int popReviveMaxAttemptTimes) {
        this.popReviveMaxAttemptTimes = popReviveMaxAttemptTimes;
    }

    public boolean isPopReviveSkipIfGroupAbsent() {
        return popReviveSkipIfGroupAbsent;
    }

    public void setPopReviveSkipIfGroupAbsent(boolean popReviveSkipIfGroupAbsent) {
        this.popReviveSkipIfGroupAbsent = popReviveSkipIfGroupAbsent;
    }

    public boolean isTraceOn() {
        return traceOn;
    }

    public void setTraceOn(final boolean traceOn) {
        this.traceOn = traceOn;
    }

    public long getStartAcceptSendRequestTimeStamp() {
        return startAcceptSendRequestTimeStamp;
    }

    public void setStartAcceptSendRequestTimeStamp(final long startAcceptSendRequestTimeStamp) {
        this.startAcceptSendRequestTimeStamp = startAcceptSendRequestTimeStamp;
    }

    public long getWaitTimeMillsInSendQueue() {
        return waitTimeMillsInSendQueue;
    }

    public void setWaitTimeMillsInSendQueue(final long waitTimeMillsInSendQueue) {
        this.waitTimeMillsInSendQueue = waitTimeMillsInSendQueue;
    }

    public long getConsumerFallbehindThreshold() {
        return consumerFallbehindThreshold;
    }

    public void setConsumerFallbehindThreshold(final long consumerFallbehindThreshold) {
        this.consumerFallbehindThreshold = consumerFallbehindThreshold;
    }

    public boolean isBrokerFastFailureEnable() {
        return brokerFastFailureEnable;
    }

    public void setBrokerFastFailureEnable(final boolean brokerFastFailureEnable) {
        this.brokerFastFailureEnable = brokerFastFailureEnable;
    }

    public long getWaitTimeMillsInPullQueue() {
        return waitTimeMillsInPullQueue;
    }

    public void setWaitTimeMillsInPullQueue(final long waitTimeMillsInPullQueue) {
        this.waitTimeMillsInPullQueue = waitTimeMillsInPullQueue;
    }

    public boolean isDisableConsumeIfConsumerReadSlowly() {
        return disableConsumeIfConsumerReadSlowly;
    }

    public void setDisableConsumeIfConsumerReadSlowly(final boolean disableConsumeIfConsumerReadSlowly) {
        this.disableConsumeIfConsumerReadSlowly = disableConsumeIfConsumerReadSlowly;
    }

    public boolean isSlaveReadEnable() {
        return slaveReadEnable;
    }

    public void setSlaveReadEnable(final boolean slaveReadEnable) {
        this.slaveReadEnable = slaveReadEnable;
    }

    public int getRegisterBrokerTimeoutMills() {
        return registerBrokerTimeoutMills;
    }

    public void setRegisterBrokerTimeoutMills(final int registerBrokerTimeoutMills) {
        this.registerBrokerTimeoutMills = registerBrokerTimeoutMills;
    }

    public String getRegionId() {
        return regionId;
    }

    public void setRegionId(final String regionId) {
        this.regionId = regionId;
    }

    public boolean isTransferMsgByHeap() {
        return transferMsgByHeap;
    }

    public void setTransferMsgByHeap(final boolean transferMsgByHeap) {
        this.transferMsgByHeap = transferMsgByHeap;
    }

    public String getMessageStorePlugIn() {
        return messageStorePlugIn;
    }

    public void setMessageStorePlugIn(String messageStorePlugIn) {
        this.messageStorePlugIn = messageStorePlugIn;
    }

    public boolean isHighSpeedMode() {
        return highSpeedMode;
    }

    public void setHighSpeedMode(final boolean highSpeedMode) {
        this.highSpeedMode = highSpeedMode;
    }

    public int getBrokerPermission() {
        return brokerPermission;
    }

    public void setBrokerPermission(int brokerPermission) {
        this.brokerPermission = brokerPermission;
    }

    public int getDefaultTopicQueueNums() {
        return defaultTopicQueueNums;
    }

    public void setDefaultTopicQueueNums(int defaultTopicQueueNums) {
        this.defaultTopicQueueNums = defaultTopicQueueNums;
    }

    public boolean isAutoCreateTopicEnable() {
        return autoCreateTopicEnable;
    }

    public void setAutoCreateTopicEnable(boolean autoCreateTopic) {
        this.autoCreateTopicEnable = autoCreateTopic;
    }

    public String getBrokerIP1() {
        return brokerIP1;
    }

    public void setBrokerIP1(String brokerIP1) {
        this.brokerIP1 = brokerIP1;
    }

    public String getBrokerIP2() {
        return brokerIP2;
    }

    public void setBrokerIP2(String brokerIP2) {
        this.brokerIP2 = brokerIP2;
    }

    public int getSendMessageThreadPoolNums() {
        return sendMessageThreadPoolNums;
    }

    public void setSendMessageThreadPoolNums(int sendMessageThreadPoolNums) {
        this.sendMessageThreadPoolNums = sendMessageThreadPoolNums;
    }

    public int getPutMessageFutureThreadPoolNums() {
        return putMessageFutureThreadPoolNums;
    }

    public void setPutMessageFutureThreadPoolNums(int putMessageFutureThreadPoolNums) {
        this.putMessageFutureThreadPoolNums = putMessageFutureThreadPoolNums;
    }

    public int getPullMessageThreadPoolNums() {
        return pullMessageThreadPoolNums;
    }

    public void setPullMessageThreadPoolNums(int pullMessageThreadPoolNums) {
        this.pullMessageThreadPoolNums = pullMessageThreadPoolNums;
    }

    public int getAckMessageThreadPoolNums() {
        return ackMessageThreadPoolNums;
    }

    public void setAckMessageThreadPoolNums(int ackMessageThreadPoolNums) {
        this.ackMessageThreadPoolNums = ackMessageThreadPoolNums;
    }

    public int getProcessReplyMessageThreadPoolNums() {
        return processReplyMessageThreadPoolNums;
    }

    public void setProcessReplyMessageThreadPoolNums(int processReplyMessageThreadPoolNums) {
        this.processReplyMessageThreadPoolNums = processReplyMessageThreadPoolNums;
    }

    public int getQueryMessageThreadPoolNums() {
        return queryMessageThreadPoolNums;
    }

    public void setQueryMessageThreadPoolNums(final int queryMessageThreadPoolNums) {
        this.queryMessageThreadPoolNums = queryMessageThreadPoolNums;
    }

    public int getAdminBrokerThreadPoolNums() {
        return adminBrokerThreadPoolNums;
    }

    public void setAdminBrokerThreadPoolNums(int adminBrokerThreadPoolNums) {
        this.adminBrokerThreadPoolNums = adminBrokerThreadPoolNums;
    }

    public int getFlushConsumerOffsetInterval() {
        return flushConsumerOffsetInterval;
    }

    public void setFlushConsumerOffsetInterval(int flushConsumerOffsetInterval) {
        this.flushConsumerOffsetInterval = flushConsumerOffsetInterval;
    }

    public int getFlushConsumerOffsetHistoryInterval() {
        return flushConsumerOffsetHistoryInterval;
    }

    public void setFlushConsumerOffsetHistoryInterval(int flushConsumerOffsetHistoryInterval) {
        this.flushConsumerOffsetHistoryInterval = flushConsumerOffsetHistoryInterval;
    }

    public boolean isClusterTopicEnable() {
        return clusterTopicEnable;
    }

    public void setClusterTopicEnable(boolean clusterTopicEnable) {
        this.clusterTopicEnable = clusterTopicEnable;
    }

    public String getNamesrvAddr() {
        return namesrvAddr;
    }

    public void setNamesrvAddr(String namesrvAddr) {
        this.namesrvAddr = namesrvAddr;
    }

    public boolean isAutoCreateSubscriptionGroup() {
        return autoCreateSubscriptionGroup;
    }

    public void setAutoCreateSubscriptionGroup(boolean autoCreateSubscriptionGroup) {
        this.autoCreateSubscriptionGroup = autoCreateSubscriptionGroup;
    }

    public String getBrokerConfigPath() {
        return brokerConfigPath;
    }

    public void setBrokerConfigPath(String brokerConfigPath) {
        this.brokerConfigPath = brokerConfigPath;
    }

    public String getRocketmqHome() {
        return rocketmqHome;
    }

    public void setRocketmqHome(String rocketmqHome) {
        this.rocketmqHome = rocketmqHome;
    }

    public int getListenPort() {
        return listenPort;
    }

    public void setListenPort(int listenPort) {
        this.listenPort = listenPort;
    }

    public int getLitePullMessageThreadPoolNums() {
        return litePullMessageThreadPoolNums;
    }

    public void setLitePullMessageThreadPoolNums(int litePullMessageThreadPoolNums) {
        this.litePullMessageThreadPoolNums = litePullMessageThreadPoolNums;
    }

    public int getLitePullThreadPoolQueueCapacity() {
        return litePullThreadPoolQueueCapacity;
    }

    public void setLitePullThreadPoolQueueCapacity(int litePullThreadPoolQueueCapacity) {
        this.litePullThreadPoolQueueCapacity = litePullThreadPoolQueueCapacity;
    }

    public int getAdminBrokerThreadPoolQueueCapacity() {
        return adminBrokerThreadPoolQueueCapacity;
    }

    public void setAdminBrokerThreadPoolQueueCapacity(int adminBrokerThreadPoolQueueCapacity) {
        this.adminBrokerThreadPoolQueueCapacity = adminBrokerThreadPoolQueueCapacity;
    }

    public int getLoadBalanceThreadPoolQueueCapacity() {
        return loadBalanceThreadPoolQueueCapacity;
    }

    public void setLoadBalanceThreadPoolQueueCapacity(int loadBalanceThreadPoolQueueCapacity) {
        this.loadBalanceThreadPoolQueueCapacity = loadBalanceThreadPoolQueueCapacity;
    }

    public int getSendHeartbeatTimeoutMillis() {
        return sendHeartbeatTimeoutMillis;
    }

    public void setSendHeartbeatTimeoutMillis(int sendHeartbeatTimeoutMillis) {
        this.sendHeartbeatTimeoutMillis = sendHeartbeatTimeoutMillis;
    }

    public long getWaitTimeMillsInLitePullQueue() {
        return waitTimeMillsInLitePullQueue;
    }

    public void setWaitTimeMillsInLitePullQueue(long waitTimeMillsInLitePullQueue) {
        this.waitTimeMillsInLitePullQueue = waitTimeMillsInLitePullQueue;
    }

    public boolean isLitePullMessageEnable() {
        return litePullMessageEnable;
    }

    public void setLitePullMessageEnable(boolean litePullMessageEnable) {
        this.litePullMessageEnable = litePullMessageEnable;
    }

    public int getSyncBrokerMemberGroupPeriod() {
        return syncBrokerMemberGroupPeriod;
    }

    public void setSyncBrokerMemberGroupPeriod(int syncBrokerMemberGroupPeriod) {
        this.syncBrokerMemberGroupPeriod = syncBrokerMemberGroupPeriod;
    }

    public boolean isRejectTransactionMessage() {
        return rejectTransactionMessage;
    }

    public void setRejectTransactionMessage(boolean rejectTransactionMessage) {
        this.rejectTransactionMessage = rejectTransactionMessage;
    }

    public boolean isFetchNamesrvAddrByAddressServer() {
        return fetchNamesrvAddrByAddressServer;
    }

    public void setFetchNamesrvAddrByAddressServer(boolean fetchNamesrvAddrByAddressServer) {
        this.fetchNamesrvAddrByAddressServer = fetchNamesrvAddrByAddressServer;
    }

    public int getSendThreadPoolQueueCapacity() {
        return sendThreadPoolQueueCapacity;
    }

    public void setSendThreadPoolQueueCapacity(int sendThreadPoolQueueCapacity) {
        this.sendThreadPoolQueueCapacity = sendThreadPoolQueueCapacity;
    }

    public int getPutThreadPoolQueueCapacity() {
        return putThreadPoolQueueCapacity;
    }

    public void setPutThreadPoolQueueCapacity(int putThreadPoolQueueCapacity) {
        this.putThreadPoolQueueCapacity = putThreadPoolQueueCapacity;
    }

    public int getPullThreadPoolQueueCapacity() {
        return pullThreadPoolQueueCapacity;
    }

    public void setPullThreadPoolQueueCapacity(int pullThreadPoolQueueCapacity) {
        this.pullThreadPoolQueueCapacity = pullThreadPoolQueueCapacity;
    }

    public int getAckThreadPoolQueueCapacity() {
        return ackThreadPoolQueueCapacity;
    }

    public void setAckThreadPoolQueueCapacity(int ackThreadPoolQueueCapacity) {
        this.ackThreadPoolQueueCapacity = ackThreadPoolQueueCapacity;
    }

    public int getReplyThreadPoolQueueCapacity() {
        return replyThreadPoolQueueCapacity;
    }

    public void setReplyThreadPoolQueueCapacity(int replyThreadPoolQueueCapacity) {
        this.replyThreadPoolQueueCapacity = replyThreadPoolQueueCapacity;
    }

    public int getQueryThreadPoolQueueCapacity() {
        return queryThreadPoolQueueCapacity;
    }

    public void setQueryThreadPoolQueueCapacity(final int queryThreadPoolQueueCapacity) {
        this.queryThreadPoolQueueCapacity = queryThreadPoolQueueCapacity;
    }

    public boolean isBrokerTopicEnable() {
        return brokerTopicEnable;
    }

    public void setBrokerTopicEnable(boolean brokerTopicEnable) {
        this.brokerTopicEnable = brokerTopicEnable;
    }

    public boolean isLongPollingEnable() {
        return longPollingEnable;
    }

    public void setLongPollingEnable(boolean longPollingEnable) {
        this.longPollingEnable = longPollingEnable;
    }

    public boolean isNotifyConsumerIdsChangedEnable() {
        return notifyConsumerIdsChangedEnable;
    }

    public void setNotifyConsumerIdsChangedEnable(boolean notifyConsumerIdsChangedEnable) {
        this.notifyConsumerIdsChangedEnable = notifyConsumerIdsChangedEnable;
    }

    public long getShortPollingTimeMills() {
        return shortPollingTimeMills;
    }

    public void setShortPollingTimeMills(long shortPollingTimeMills) {
        this.shortPollingTimeMills = shortPollingTimeMills;
    }

    public int getClientManageThreadPoolNums() {
        return clientManageThreadPoolNums;
    }

    public void setClientManageThreadPoolNums(int clientManageThreadPoolNums) {
        this.clientManageThreadPoolNums = clientManageThreadPoolNums;
    }

    public int getClientManagerThreadPoolQueueCapacity() {
        return clientManagerThreadPoolQueueCapacity;
    }

    public void setClientManagerThreadPoolQueueCapacity(int clientManagerThreadPoolQueueCapacity) {
        this.clientManagerThreadPoolQueueCapacity = clientManagerThreadPoolQueueCapacity;
    }

    public int getConsumerManagerThreadPoolQueueCapacity() {
        return consumerManagerThreadPoolQueueCapacity;
    }

    public void setConsumerManagerThreadPoolQueueCapacity(int consumerManagerThreadPoolQueueCapacity) {
        this.consumerManagerThreadPoolQueueCapacity = consumerManagerThreadPoolQueueCapacity;
    }

    public int getConsumerManageThreadPoolNums() {
        return consumerManageThreadPoolNums;
    }

    public void setConsumerManageThreadPoolNums(int consumerManageThreadPoolNums) {
        this.consumerManageThreadPoolNums = consumerManageThreadPoolNums;
    }

    public int getCommercialBaseCount() {
        return commercialBaseCount;
    }

    public void setCommercialBaseCount(int commercialBaseCount) {
        this.commercialBaseCount = commercialBaseCount;
    }

    public boolean isEnableCalcFilterBitMap() {
        return enableCalcFilterBitMap;
    }

    public void setEnableCalcFilterBitMap(boolean enableCalcFilterBitMap) {
        this.enableCalcFilterBitMap = enableCalcFilterBitMap;
    }

    public int getExpectConsumerNumUseFilter() {
        return expectConsumerNumUseFilter;
    }

    public void setExpectConsumerNumUseFilter(int expectConsumerNumUseFilter) {
        this.expectConsumerNumUseFilter = expectConsumerNumUseFilter;
    }

    public int getMaxErrorRateOfBloomFilter() {
        return maxErrorRateOfBloomFilter;
    }

    public void setMaxErrorRateOfBloomFilter(int maxErrorRateOfBloomFilter) {
        this.maxErrorRateOfBloomFilter = maxErrorRateOfBloomFilter;
    }

    public long getFilterDataCleanTimeSpan() {
        return filterDataCleanTimeSpan;
    }

    public void setFilterDataCleanTimeSpan(long filterDataCleanTimeSpan) {
        this.filterDataCleanTimeSpan = filterDataCleanTimeSpan;
    }

    public boolean isFilterSupportRetry() {
        return filterSupportRetry;
    }

    public void setFilterSupportRetry(boolean filterSupportRetry) {
        this.filterSupportRetry = filterSupportRetry;
    }

    public boolean isEnablePropertyFilter() {
        return enablePropertyFilter;
    }

    public void setEnablePropertyFilter(boolean enablePropertyFilter) {
        this.enablePropertyFilter = enablePropertyFilter;
    }

    public boolean isCompressedRegister() {
        return compressedRegister;
    }

    public void setCompressedRegister(boolean compressedRegister) {
        this.compressedRegister = compressedRegister;
    }

    public boolean isForceRegister() {
        return forceRegister;
    }

    public void setForceRegister(boolean forceRegister) {
        this.forceRegister = forceRegister;
    }

    public int getHeartbeatThreadPoolQueueCapacity() {
        return heartbeatThreadPoolQueueCapacity;
    }

    public void setHeartbeatThreadPoolQueueCapacity(int heartbeatThreadPoolQueueCapacity) {
        this.heartbeatThreadPoolQueueCapacity = heartbeatThreadPoolQueueCapacity;
    }

    public int getHeartbeatThreadPoolNums() {
        return heartbeatThreadPoolNums;
    }

    public void setHeartbeatThreadPoolNums(int heartbeatThreadPoolNums) {
        this.heartbeatThreadPoolNums = heartbeatThreadPoolNums;
    }

    public long getWaitTimeMillsInHeartbeatQueue() {
        return waitTimeMillsInHeartbeatQueue;
    }

    public void setWaitTimeMillsInHeartbeatQueue(long waitTimeMillsInHeartbeatQueue) {
        this.waitTimeMillsInHeartbeatQueue = waitTimeMillsInHeartbeatQueue;
    }

    public int getRegisterNameServerPeriod() {
        return registerNameServerPeriod;
    }

    public void setRegisterNameServerPeriod(int registerNameServerPeriod) {
        this.registerNameServerPeriod = registerNameServerPeriod;
    }

    public long getTransactionTimeOut() {
        return transactionTimeOut;
    }

    public void setTransactionTimeOut(long transactionTimeOut) {
        this.transactionTimeOut = transactionTimeOut;
    }

    public int getTransactionCheckMax() {
        return transactionCheckMax;
    }

    public void setTransactionCheckMax(int transactionCheckMax) {
        this.transactionCheckMax = transactionCheckMax;
    }

    public long getTransactionCheckInterval() {
        return transactionCheckInterval;
    }

    public void setTransactionCheckInterval(long transactionCheckInterval) {
        this.transactionCheckInterval = transactionCheckInterval;
    }

    public int getEndTransactionThreadPoolNums() {
        return endTransactionThreadPoolNums;
    }

    public void setEndTransactionThreadPoolNums(int endTransactionThreadPoolNums) {
        this.endTransactionThreadPoolNums = endTransactionThreadPoolNums;
    }

    public int getEndTransactionPoolQueueCapacity() {
        return endTransactionPoolQueueCapacity;
    }

    public void setEndTransactionPoolQueueCapacity(int endTransactionPoolQueueCapacity) {
        this.endTransactionPoolQueueCapacity = endTransactionPoolQueueCapacity;
    }

    public long getWaitTimeMillsInTransactionQueue() {
        return waitTimeMillsInTransactionQueue;
    }

    public void setWaitTimeMillsInTransactionQueue(long waitTimeMillsInTransactionQueue) {
        this.waitTimeMillsInTransactionQueue = waitTimeMillsInTransactionQueue;
    }

    public String getMsgTraceTopicName() {
        return msgTraceTopicName;
    }

    public long getWaitTimeMillsInAdminBrokerQueue() {
        return waitTimeMillsInAdminBrokerQueue;
    }

    public void setWaitTimeMillsInAdminBrokerQueue(long waitTimeMillsInAdminBrokerQueue) {
        this.waitTimeMillsInAdminBrokerQueue = waitTimeMillsInAdminBrokerQueue;
    }

    public void setMsgTraceTopicName(String msgTraceTopicName) {
        this.msgTraceTopicName = msgTraceTopicName;
    }

    public boolean isTraceTopicEnable() {
        return traceTopicEnable;
    }

    public void setTraceTopicEnable(boolean traceTopicEnable) {
        this.traceTopicEnable = traceTopicEnable;
    }

    public void setAclEnable(boolean aclEnable) {
        this.aclEnable = aclEnable;
    }

    public boolean isStoreReplyMessageEnable() {
        return storeReplyMessageEnable;
    }

    public void setStoreReplyMessageEnable(boolean storeReplyMessageEnable) {
        this.storeReplyMessageEnable = storeReplyMessageEnable;
    }

    public boolean isEnableDetailStat() {
        return enableDetailStat;
    }

    public void setEnableDetailStat(boolean enableDetailStat) {
        this.enableDetailStat = enableDetailStat;
    }

    public boolean isAutoDeleteUnusedStats() {
        return autoDeleteUnusedStats;
    }

    public void setAutoDeleteUnusedStats(boolean autoDeleteUnusedStats) {
        this.autoDeleteUnusedStats = autoDeleteUnusedStats;
    }

    public long getLoadBalancePollNameServerInterval() {
        return loadBalancePollNameServerInterval;
    }

    public void setLoadBalancePollNameServerInterval(long loadBalancePollNameServerInterval) {
        this.loadBalancePollNameServerInterval = loadBalancePollNameServerInterval;
    }

    public int getCleanOfflineBrokerInterval() {
        return cleanOfflineBrokerInterval;
    }

    public void setCleanOfflineBrokerInterval(int cleanOfflineBrokerInterval) {
        this.cleanOfflineBrokerInterval = cleanOfflineBrokerInterval;
    }

    public int getLoadBalanceProcessorThreadPoolNums() {
        return loadBalanceProcessorThreadPoolNums;
    }

    public void setLoadBalanceProcessorThreadPoolNums(int loadBalanceProcessorThreadPoolNums) {
        this.loadBalanceProcessorThreadPoolNums = loadBalanceProcessorThreadPoolNums;
    }

    public boolean isServerLoadBalancerEnable() {
        return serverLoadBalancerEnable;
    }

    public void setServerLoadBalancerEnable(boolean serverLoadBalancerEnable) {
        this.serverLoadBalancerEnable = serverLoadBalancerEnable;
    }

    public MessageRequestMode getDefaultMessageRequestMode() {
        return defaultMessageRequestMode;
    }

    public void setDefaultMessageRequestMode(String defaultMessageRequestMode) {
        this.defaultMessageRequestMode = MessageRequestMode.valueOf(defaultMessageRequestMode);
    }

    public int getDefaultPopShareQueueNum() {
        return defaultPopShareQueueNum;
    }

    public void setDefaultPopShareQueueNum(int defaultPopShareQueueNum) {
        this.defaultPopShareQueueNum = defaultPopShareQueueNum;
    }

    public long getForwardTimeout() {
        return forwardTimeout;
    }

    public void setForwardTimeout(long timeout) {
        this.forwardTimeout = timeout;
    }

    public int getBrokerHeartbeatInterval() {
        return brokerHeartbeatInterval;
    }

    public void setBrokerHeartbeatInterval(int brokerHeartbeatInterval) {
        this.brokerHeartbeatInterval = brokerHeartbeatInterval;
    }

    public long getBrokerNotActiveTimeoutMillis() {
        return brokerNotActiveTimeoutMillis;
    }

    public void setBrokerNotActiveTimeoutMillis(long brokerNotActiveTimeoutMillis) {
        this.brokerNotActiveTimeoutMillis = brokerNotActiveTimeoutMillis;
    }

    public boolean isEnableNetWorkFlowControl() {
        return enableNetWorkFlowControl;
    }

    public void setEnableNetWorkFlowControl(boolean enableNetWorkFlowControl) {
        this.enableNetWorkFlowControl = enableNetWorkFlowControl;
    }

    public long getPopLongPollingForceNotifyInterval() {
        return popLongPollingForceNotifyInterval;
    }

    public void setPopLongPollingForceNotifyInterval(long popLongPollingForceNotifyInterval) {
        this.popLongPollingForceNotifyInterval = popLongPollingForceNotifyInterval;
    }

    public boolean isEnableNotifyBeforePopCalculateLag() {
        return enableNotifyBeforePopCalculateLag;
    }

    public void setEnableNotifyBeforePopCalculateLag(boolean enableNotifyBeforePopCalculateLag) {
        this.enableNotifyBeforePopCalculateLag = enableNotifyBeforePopCalculateLag;
    }

    public boolean isEnableNotifyAfterPopOrderLockRelease() {
        return enableNotifyAfterPopOrderLockRelease;
    }

    public void setEnableNotifyAfterPopOrderLockRelease(boolean enableNotifyAfterPopOrderLockRelease) {
        this.enableNotifyAfterPopOrderLockRelease = enableNotifyAfterPopOrderLockRelease;
    }

    public boolean isInitPopOffsetByCheckMsgInMem() {
        return initPopOffsetByCheckMsgInMem;
    }

    public void setInitPopOffsetByCheckMsgInMem(boolean initPopOffsetByCheckMsgInMem) {
        this.initPopOffsetByCheckMsgInMem = initPopOffsetByCheckMsgInMem;
    }

    public boolean isRetrieveMessageFromPopRetryTopicV1() {
        return retrieveMessageFromPopRetryTopicV1;
    }

    public void setRetrieveMessageFromPopRetryTopicV1(boolean retrieveMessageFromPopRetryTopicV1) {
        this.retrieveMessageFromPopRetryTopicV1 = retrieveMessageFromPopRetryTopicV1;
    }

    public boolean isEnableRetryTopicV2() {
        return enableRetryTopicV2;
    }

    public void setEnableRetryTopicV2(boolean enableRetryTopicV2) {
        this.enableRetryTopicV2 = enableRetryTopicV2;
    }

    public boolean isRealTimeNotifyConsumerChange() {
        return realTimeNotifyConsumerChange;
    }

    public void setRealTimeNotifyConsumerChange(boolean realTimeNotifyConsumerChange) {
        this.realTimeNotifyConsumerChange = realTimeNotifyConsumerChange;
    }

    public boolean isEnableSlaveActingMaster() {
        return enableSlaveActingMaster;
    }

    public void setEnableSlaveActingMaster(boolean enableSlaveActingMaster) {
        this.enableSlaveActingMaster = enableSlaveActingMaster;
    }

    public boolean isEnableRemoteEscape() {
        return enableRemoteEscape;
    }

    public void setEnableRemoteEscape(boolean enableRemoteEscape) {
        this.enableRemoteEscape = enableRemoteEscape;
    }

    public boolean isSkipPreOnline() {
        return skipPreOnline;
    }

    public void setSkipPreOnline(boolean skipPreOnline) {
        this.skipPreOnline = skipPreOnline;
    }

    public boolean isAsyncSendEnable() {
        return asyncSendEnable;
    }

    public void setAsyncSendEnable(boolean asyncSendEnable) {
        this.asyncSendEnable = asyncSendEnable;
    }

    public long getConsumerOffsetUpdateVersionStep() {
        return consumerOffsetUpdateVersionStep;
    }

    public void setConsumerOffsetUpdateVersionStep(long consumerOffsetUpdateVersionStep) {
        this.consumerOffsetUpdateVersionStep = consumerOffsetUpdateVersionStep;
    }

    public long getDelayOffsetUpdateVersionStep() {
        return delayOffsetUpdateVersionStep;
    }

    public void setDelayOffsetUpdateVersionStep(long delayOffsetUpdateVersionStep) {
        this.delayOffsetUpdateVersionStep = delayOffsetUpdateVersionStep;
    }

    public int getCommercialSizePerMsg() {
        return commercialSizePerMsg;
    }

    public void setCommercialSizePerMsg(int commercialSizePerMsg) {
        this.commercialSizePerMsg = commercialSizePerMsg;
    }

    public long getWaitTimeMillsInAckQueue() {
        return waitTimeMillsInAckQueue;
    }

    public void setWaitTimeMillsInAckQueue(long waitTimeMillsInAckQueue) {
        this.waitTimeMillsInAckQueue = waitTimeMillsInAckQueue;
    }

    public boolean isRejectPullConsumerEnable() {
        return rejectPullConsumerEnable;
    }

    public void setRejectPullConsumerEnable(boolean rejectPullConsumerEnable) {
        this.rejectPullConsumerEnable = rejectPullConsumerEnable;
    }

    public boolean isAccountStatsEnable() {
        return accountStatsEnable;
    }

    public void setAccountStatsEnable(boolean accountStatsEnable) {
        this.accountStatsEnable = accountStatsEnable;
    }

    public boolean isAccountStatsPrintZeroValues() {
        return accountStatsPrintZeroValues;
    }

    public void setAccountStatsPrintZeroValues(boolean accountStatsPrintZeroValues) {
        this.accountStatsPrintZeroValues = accountStatsPrintZeroValues;
    }

    public int getMaxStatsIdleTimeInMinutes() {
        return maxStatsIdleTimeInMinutes;
    }

    public void setMaxStatsIdleTimeInMinutes(int maxStatsIdleTimeInMinutes) {
        this.maxStatsIdleTimeInMinutes = maxStatsIdleTimeInMinutes;
    }

    public boolean isLockInStrictMode() {
        return lockInStrictMode;
    }

    public void setLockInStrictMode(boolean lockInStrictMode) {
        this.lockInStrictMode = lockInStrictMode;
    }

    public boolean isIsolateLogEnable() {
        return isolateLogEnable;
    }

    public void setIsolateLogEnable(boolean isolateLogEnable) {
        this.isolateLogEnable = isolateLogEnable;
    }

    public boolean isCompatibleWithOldNameSrv() {
        return compatibleWithOldNameSrv;
    }

    public void setCompatibleWithOldNameSrv(boolean compatibleWithOldNameSrv) {
        this.compatibleWithOldNameSrv = compatibleWithOldNameSrv;
    }

    public boolean isEnableControllerMode() {
        return enableControllerMode;
    }

    public void setEnableControllerMode(boolean enableControllerMode) {
        this.enableControllerMode = enableControllerMode;
    }

    public String getControllerAddr() {
        return controllerAddr;
    }

    public void setControllerAddr(String controllerAddr) {
        this.controllerAddr = controllerAddr;
    }

    public boolean isFetchControllerAddrByDnsLookup() {
        return fetchControllerAddrByDnsLookup;
    }

    public void setFetchControllerAddrByDnsLookup(boolean fetchControllerAddrByDnsLookup) {
        this.fetchControllerAddrByDnsLookup = fetchControllerAddrByDnsLookup;
    }

    public long getSyncBrokerMetadataPeriod() {
        return syncBrokerMetadataPeriod;
    }

    public void setSyncBrokerMetadataPeriod(long syncBrokerMetadataPeriod) {
        this.syncBrokerMetadataPeriod = syncBrokerMetadataPeriod;
    }

    public long getCheckSyncStateSetPeriod() {
        return checkSyncStateSetPeriod;
    }

    public void setCheckSyncStateSetPeriod(long checkSyncStateSetPeriod) {
        this.checkSyncStateSetPeriod = checkSyncStateSetPeriod;
    }

    public long getSyncControllerMetadataPeriod() {
        return syncControllerMetadataPeriod;
    }

    public void setSyncControllerMetadataPeriod(long syncControllerMetadataPeriod) {
        this.syncControllerMetadataPeriod = syncControllerMetadataPeriod;
    }

    public int getBrokerElectionPriority() {
        return brokerElectionPriority;
    }

    public void setBrokerElectionPriority(int brokerElectionPriority) {
        this.brokerElectionPriority = brokerElectionPriority;
    }

    public long getControllerHeartBeatTimeoutMills() {
        return controllerHeartBeatTimeoutMills;
    }

    public void setControllerHeartBeatTimeoutMills(long controllerHeartBeatTimeoutMills) {
        this.controllerHeartBeatTimeoutMills = controllerHeartBeatTimeoutMills;
    }

    public boolean isRecoverConcurrently() {
        return recoverConcurrently;
    }

    public void setRecoverConcurrently(boolean recoverConcurrently) {
        this.recoverConcurrently = recoverConcurrently;
    }

    public int getRecoverThreadPoolNums() {
        return recoverThreadPoolNums;
    }

    public void setRecoverThreadPoolNums(int recoverThreadPoolNums) {
        this.recoverThreadPoolNums = recoverThreadPoolNums;
    }

    public boolean isFetchNameSrvAddrByDnsLookup() {
        return fetchNameSrvAddrByDnsLookup;
    }

    public void setFetchNameSrvAddrByDnsLookup(boolean fetchNameSrvAddrByDnsLookup) {
        this.fetchNameSrvAddrByDnsLookup = fetchNameSrvAddrByDnsLookup;
    }

    public boolean isUseServerSideResetOffset() {
        return useServerSideResetOffset;
    }

    public void setUseServerSideResetOffset(boolean useServerSideResetOffset) {
        this.useServerSideResetOffset = useServerSideResetOffset;
    }

    public boolean isEnableBroadcastOffsetStore() {
        return enableBroadcastOffsetStore;
    }

    public void setEnableBroadcastOffsetStore(boolean enableBroadcastOffsetStore) {
        this.enableBroadcastOffsetStore = enableBroadcastOffsetStore;
    }

    public long getBroadcastOffsetExpireSecond() {
        return broadcastOffsetExpireSecond;
    }

    public void setBroadcastOffsetExpireSecond(long broadcastOffsetExpireSecond) {
        this.broadcastOffsetExpireSecond = broadcastOffsetExpireSecond;
    }

    public long getBroadcastOffsetExpireMaxSecond() {
        return broadcastOffsetExpireMaxSecond;
    }

    public void setBroadcastOffsetExpireMaxSecond(long broadcastOffsetExpireMaxSecond) {
        this.broadcastOffsetExpireMaxSecond = broadcastOffsetExpireMaxSecond;
    }

    public MetricsExporterType getMetricsExporterType() {
        return metricsExporterType;
    }

    public void setMetricsExporterType(MetricsExporterType metricsExporterType) {
        this.metricsExporterType = metricsExporterType;
    }

    public void setMetricsExporterType(int metricsExporterType) {
        this.metricsExporterType = MetricsExporterType.valueOf(metricsExporterType);
    }

    public void setMetricsExporterType(String metricsExporterType) {
        this.metricsExporterType = MetricsExporterType.valueOf(metricsExporterType);
    }

    public int getMetricsOtelCardinalityLimit() {
        return metricsOtelCardinalityLimit;
    }

    public void setMetricsOtelCardinalityLimit(int metricsOtelCardinalityLimit) {
        this.metricsOtelCardinalityLimit = metricsOtelCardinalityLimit;
    }

    public String getMetricsGrpcExporterTarget() {
        return metricsGrpcExporterTarget;
    }

    public void setMetricsGrpcExporterTarget(String metricsGrpcExporterTarget) {
        this.metricsGrpcExporterTarget = metricsGrpcExporterTarget;
    }

    public String getMetricsGrpcExporterHeader() {
        return metricsGrpcExporterHeader;
    }

    public void setMetricsGrpcExporterHeader(String metricsGrpcExporterHeader) {
        this.metricsGrpcExporterHeader = metricsGrpcExporterHeader;
    }

    public long getMetricGrpcExporterTimeOutInMills() {
        return metricGrpcExporterTimeOutInMills;
    }

    public void setMetricGrpcExporterTimeOutInMills(long metricGrpcExporterTimeOutInMills) {
        this.metricGrpcExporterTimeOutInMills = metricGrpcExporterTimeOutInMills;
    }

    public long getMetricGrpcExporterIntervalInMills() {
        return metricGrpcExporterIntervalInMills;
    }

    public void setMetricGrpcExporterIntervalInMills(long metricGrpcExporterIntervalInMills) {
        this.metricGrpcExporterIntervalInMills = metricGrpcExporterIntervalInMills;
    }

    public long getMetricLoggingExporterIntervalInMills() {
        return metricLoggingExporterIntervalInMills;
    }

    public void setMetricLoggingExporterIntervalInMills(long metricLoggingExporterIntervalInMills) {
        this.metricLoggingExporterIntervalInMills = metricLoggingExporterIntervalInMills;
    }

    public String getMetricsLabel() {
        return metricsLabel;
    }

    public void setMetricsLabel(String metricsLabel) {
        this.metricsLabel = metricsLabel;
    }

    public boolean isMetricsInDelta() {
        return metricsInDelta;
    }

    public void setMetricsInDelta(boolean metricsInDelta) {
        this.metricsInDelta = metricsInDelta;
    }

    public boolean isMetricsExportBatchSplitEnabled() {
        return metricsExportBatchSplitEnabled;
    }

    public void setMetricsExportBatchSplitEnabled(boolean metricsExportBatchSplitEnabled) {
        this.metricsExportBatchSplitEnabled = metricsExportBatchSplitEnabled;
    }

    public int getMetricsExportBatchMaxDataPoints() {
        return metricsExportBatchMaxDataPoints;
    }

    public void setMetricsExportBatchMaxDataPoints(int metricsExportBatchMaxDataPoints) {
        this.metricsExportBatchMaxDataPoints = metricsExportBatchMaxDataPoints;
    }

    public int getMetricsExportBatchMaxConcurrent() {
        return metricsExportBatchMaxConcurrent;
    }

    public void setMetricsExportBatchMaxConcurrent(int metricsExportBatchMaxConcurrent) {
        this.metricsExportBatchMaxConcurrent = metricsExportBatchMaxConcurrent;
    }

    public String getMetricsExportOtelMemoryMode() {
        return metricsExportOtelMemoryMode;
    }

    public void setMetricsExportOtelMemoryMode(String metricsExportOtelMemoryMode) {
        this.metricsExportOtelMemoryMode = metricsExportOtelMemoryMode;
    }

    public int getMetricsPromExporterPort() {
        return metricsPromExporterPort;
    }

    public void setMetricsPromExporterPort(int metricsPromExporterPort) {
        this.metricsPromExporterPort = metricsPromExporterPort;
    }

    public String getMetricsPromExporterHost() {
        return metricsPromExporterHost;
    }

    public void setMetricsPromExporterHost(String metricsPromExporterHost) {
        this.metricsPromExporterHost = metricsPromExporterHost;
    }

    public boolean isEnablePopMetrics() {
        return enablePopMetrics;
    }

    public void setEnablePopMetrics(boolean enablePopMetrics) {
        this.enablePopMetrics = enablePopMetrics;
    }

    public boolean isEnableConnectionMetrics() {
        return enableConnectionMetrics;
    }

    public void setEnableConnectionMetrics(boolean enableConnectionMetrics) {
        this.enableConnectionMetrics = enableConnectionMetrics;
    }

    public boolean isEnableTransactionMetrics() {
        return enableTransactionMetrics;
    }

    public void setEnableTransactionMetrics(boolean enableTransactionMetrics) {
        this.enableTransactionMetrics = enableTransactionMetrics;
    }

    public boolean isEnableStatsMetrics() {
        return enableStatsMetrics;
    }

    public void setEnableStatsMetrics(boolean enableStatsMetrics) {
        this.enableStatsMetrics = enableStatsMetrics;
    }

    public boolean isEnableRequestMetrics() {
        return enableRequestMetrics;
    }

    public void setEnableRequestMetrics(boolean enableRequestMetrics) {
        this.enableRequestMetrics = enableRequestMetrics;
    }


    public boolean isEnableLagAndDlqMetrics() {
        return enableLagAndDlqMetrics;
    }

    public void setEnableLagAndDlqMetrics(boolean enableLagAndDlqMetrics) {
        this.enableLagAndDlqMetrics = enableLagAndDlqMetrics;
    }

    public boolean isEnableRemotingMetrics() {
        return enableRemotingMetrics;
    }

    public void setEnableRemotingMetrics(boolean enableRemotingMetrics) {
        this.enableRemotingMetrics = enableRemotingMetrics;
    }

    public boolean isEnableMessageStoreMetrics() {
        return enableMessageStoreMetrics;
    }

    public void setEnableMessageStoreMetrics(boolean enableMessageStoreMetrics) {
        this.enableMessageStoreMetrics = enableMessageStoreMetrics;
    }

    public int getTransactionOpMsgMaxSize() {
        return transactionOpMsgMaxSize;
    }

    public void setTransactionOpMsgMaxSize(int transactionOpMsgMaxSize) {
        this.transactionOpMsgMaxSize = transactionOpMsgMaxSize;
    }

    public int getTransactionOpBatchInterval() {
        return transactionOpBatchInterval;
    }

    public void setTransactionOpBatchInterval(int transactionOpBatchInterval) {
        this.transactionOpBatchInterval = transactionOpBatchInterval;
    }

    public long getChannelExpiredTimeout() {
        return channelExpiredTimeout;
    }

    public void setChannelExpiredTimeout(long channelExpiredTimeout) {
        this.channelExpiredTimeout = channelExpiredTimeout;
    }

    public long getSubscriptionExpiredTimeout() {
        return subscriptionExpiredTimeout;
    }

    public void setSubscriptionExpiredTimeout(long subscriptionExpiredTimeout) {
        this.subscriptionExpiredTimeout = subscriptionExpiredTimeout;
    }

    public boolean isValidateSystemTopicWhenUpdateTopic() {
        return validateSystemTopicWhenUpdateTopic;
    }

    public void setValidateSystemTopicWhenUpdateTopic(boolean validateSystemTopicWhenUpdateTopic) {
        this.validateSystemTopicWhenUpdateTopic = validateSystemTopicWhenUpdateTopic;
    }

    public double getBatchDeleteTopicMaxRate() {
        return batchDeleteTopicMaxRate;
    }

    public void setBatchDeleteTopicMaxRate(double batchDeleteTopicMaxRate) {
        this.batchDeleteTopicMaxRate = batchDeleteTopicMaxRate;
    }

    public double getBatchDeleteSubscriptionGroupMaxRate() {
        return batchDeleteSubscriptionGroupMaxRate;
    }

    public void setBatchDeleteSubscriptionGroupMaxRate(double batchDeleteSubscriptionGroupMaxRate) {
        this.batchDeleteSubscriptionGroupMaxRate = batchDeleteSubscriptionGroupMaxRate;
    }

    public boolean isEstimateAccumulation() {
        return estimateAccumulation;
    }

    public void setEstimateAccumulation(boolean estimateAccumulation) {
        this.estimateAccumulation = estimateAccumulation;
    }

    public boolean isColdCtrStrategyEnable() {
        return coldCtrStrategyEnable;
    }

    public void setColdCtrStrategyEnable(boolean coldCtrStrategyEnable) {
        this.coldCtrStrategyEnable = coldCtrStrategyEnable;
    }

    public boolean isUsePIDColdCtrStrategy() {
        return usePIDColdCtrStrategy;
    }

    public void setUsePIDColdCtrStrategy(boolean usePIDColdCtrStrategy) {
        this.usePIDColdCtrStrategy = usePIDColdCtrStrategy;
    }

    public long getCgColdReadThreshold() {
        return cgColdReadThreshold;
    }

    public void setCgColdReadThreshold(long cgColdReadThreshold) {
        this.cgColdReadThreshold = cgColdReadThreshold;
    }

    public long getGlobalColdReadThreshold() {
        return globalColdReadThreshold;
    }

    public void setGlobalColdReadThreshold(long globalColdReadThreshold) {
        this.globalColdReadThreshold = globalColdReadThreshold;
    }

    public boolean isUseStaticSubscription() {
        return useStaticSubscription;
    }

    public void setUseStaticSubscription(boolean useStaticSubscription) {
        this.useStaticSubscription = useStaticSubscription;
    }

    public long getFetchNamesrvAddrInterval() {
        return fetchNamesrvAddrInterval;
    }

    public void setFetchNamesrvAddrInterval(final long fetchNamesrvAddrInterval) {
        this.fetchNamesrvAddrInterval = fetchNamesrvAddrInterval;
    }

    public boolean isPopResponseReturnActualRetryTopic() {
        return popResponseReturnActualRetryTopic;
    }

    public void setPopResponseReturnActualRetryTopic(boolean popResponseReturnActualRetryTopic) {
        this.popResponseReturnActualRetryTopic = popResponseReturnActualRetryTopic;
    }

    public boolean isEnableSingleTopicRegister() {
        return enableSingleTopicRegister;
    }

    public void setEnableSingleTopicRegister(boolean enableSingleTopicRegister) {
        this.enableSingleTopicRegister = enableSingleTopicRegister;
    }

    public boolean isEnableMixedMessageType() {
        return enableMixedMessageType;
    }

    public void setEnableMixedMessageType(boolean enableMixedMessageType) {
        this.enableMixedMessageType = enableMixedMessageType;
    }

    public boolean isEnableSplitRegistration() {
        return enableSplitRegistration;
    }

    public void setEnableSplitRegistration(boolean enableSplitRegistration) {
        this.enableSplitRegistration = enableSplitRegistration;
    }

    public boolean isEnableFastChannelEventProcess() {
        return enableFastChannelEventProcess;
    }

    public void setEnableFastChannelEventProcess(boolean enableFastChannelEventProcess) {
        this.enableFastChannelEventProcess = enableFastChannelEventProcess;
    }

    public boolean isPrintChannelGroups() {
        return printChannelGroups;
    }

    public void setPrintChannelGroups(boolean printChannelGroups) {
        this.printChannelGroups = printChannelGroups;
    }

    public int getPrintChannelGroupsMinNum() {
        return printChannelGroupsMinNum;
    }

    public void setPrintChannelGroupsMinNum(int printChannelGroupsMinNum) {
        this.printChannelGroupsMinNum = printChannelGroupsMinNum;
    }

    public int getSplitRegistrationSize() {
        return splitRegistrationSize;
    }

    public void setSplitRegistrationSize(int splitRegistrationSize) {
        this.splitRegistrationSize = splitRegistrationSize;
    }

    public long getTransactionMetricFlushInterval() {
        return transactionMetricFlushInterval;
    }

    public void setTransactionMetricFlushInterval(long transactionMetricFlushInterval) {
        this.transactionMetricFlushInterval = transactionMetricFlushInterval;
    }

    public void setTransactionCheckRocksdbCoreThreads(int transactionCheckRocksdbCoreThreads) {
        this.transactionCheckRocksdbCoreThreads = transactionCheckRocksdbCoreThreads;
    }

    public int getTransactionCheckRocksdbCoreThreads() {
        return transactionCheckRocksdbCoreThreads;
    }

    public int getTransactionCheckRocksdbMaxThreads() {
        return transactionCheckRocksdbMaxThreads;
    }

    public void setTransactionCheckRocksdbMaxThreads(int transactionCheckRocksdbMaxThreads) {
        this.transactionCheckRocksdbMaxThreads = transactionCheckRocksdbMaxThreads;
    }

    public int getTransactionCheckRocksdbQueueCapacity() {
        return transactionCheckRocksdbQueueCapacity;
    }

    public void setTransactionCheckRocksdbQueueCapacity(int transactionCheckRocksdbQueueCapacity) {
        this.transactionCheckRocksdbQueueCapacity = transactionCheckRocksdbQueueCapacity;
    }

    public long getPopInflightMessageThreshold() {
        return popInflightMessageThreshold;
    }

    public void setPopInflightMessageThreshold(long popInflightMessageThreshold) {
        this.popInflightMessageThreshold = popInflightMessageThreshold;
    }

    public boolean isEnablePopMessageThreshold() {
        return enablePopMessageThreshold;
    }

    public void setEnablePopMessageThreshold(boolean enablePopMessageThreshold) {
        this.enablePopMessageThreshold = enablePopMessageThreshold;
    }

    public boolean isSkipWhenCKRePutReachMaxTimes() {
        return skipWhenCKRePutReachMaxTimes;
    }

    public void setSkipWhenCKRePutReachMaxTimes(boolean skipWhenCKRePutReachMaxTimes) {
        this.skipWhenCKRePutReachMaxTimes = skipWhenCKRePutReachMaxTimes;
    }

    public int getUpdateNameServerAddrPeriod() {
        return updateNameServerAddrPeriod;
    }

    public void setUpdateNameServerAddrPeriod(int updateNameServerAddrPeriod) {
        this.updateNameServerAddrPeriod = updateNameServerAddrPeriod;
    }

    public boolean isAppendAckAsync() {
        return appendAckAsync;
    }

    public void setAppendAckAsync(boolean appendAckAsync) {
        this.appendAckAsync = appendAckAsync;
    }

    public boolean isAppendCkAsync() {
        return appendCkAsync;
    }

    public void setAppendCkAsync(boolean appendCkAsync) {
        this.appendCkAsync = appendCkAsync;
    }

    public boolean isClearRetryTopicWhenDeleteTopic() {
        return clearRetryTopicWhenDeleteTopic;
    }

    public void setClearRetryTopicWhenDeleteTopic(boolean clearRetryTopicWhenDeleteTopic) {
        this.clearRetryTopicWhenDeleteTopic = clearRetryTopicWhenDeleteTopic;
    }

    public boolean isEnableLmqStats() {
        return enableLmqStats;
    }

    public void setEnableLmqStats(boolean enableLmqStats) {
        this.enableLmqStats = enableLmqStats;
    }

    public String getConfigManagerVersion() {
        return configManagerVersion;
    }

    public void setConfigManagerVersion(String configManagerVersion) {
        this.configManagerVersion = configManagerVersion;
    }

    public boolean isUseSingleRocksDBForAllConfigs() {
        return useSingleRocksDBForAllConfigs;
    }

    public void setUseSingleRocksDBForAllConfigs(boolean useSingleRocksDBForAllConfigs) {
        this.useSingleRocksDBForAllConfigs = useSingleRocksDBForAllConfigs;
    }

    public boolean isAllowRecallWhenBrokerNotWriteable() {
        return allowRecallWhenBrokerNotWriteable;
    }

    public void setAllowRecallWhenBrokerNotWriteable(boolean allowRecallWhenBrokerNotWriteable) {
        this.allowRecallWhenBrokerNotWriteable = allowRecallWhenBrokerNotWriteable;
    }

    public boolean isRecallMessageEnable() {
        return recallMessageEnable;
    }

    public void setRecallMessageEnable(boolean recallMessageEnable) {
        this.recallMessageEnable = recallMessageEnable;
    }

    public boolean isEnableRegisterProducer() {
        return enableRegisterProducer;
    }

    public void setEnableRegisterProducer(boolean enableRegisterProducer) {
        this.enableRegisterProducer = enableRegisterProducer;
    }

    public boolean isEnableCreateSysGroup() {
        return enableCreateSysGroup;
    }

    public void setEnableCreateSysGroup(boolean enableCreateSysGroup) {
        this.enableCreateSysGroup = enableCreateSysGroup;
    }

    public boolean isEnableSplitMetadata() {
        return enableSplitMetadata;
    }

    public void setEnableSplitMetadata(boolean enableSplitMetadata) {
        this.enableSplitMetadata = enableSplitMetadata;
    }

    public int getSplitMetadataSize() {
        return splitMetadataSize;
    }

    public void setSplitMetadataSize(int splitMetadataSize) {
        this.splitMetadataSize = splitMetadataSize;
    }

    public int getPopFromRetryProbabilityForPriority() {
        return popFromRetryProbabilityForPriority;
    }

    public void setPopFromRetryProbabilityForPriority(int popFromRetryProbabilityForPriority) {
        this.popFromRetryProbabilityForPriority = popFromRetryProbabilityForPriority;
    }

    public boolean isPriorityOrderAsc() {
        return priorityOrderAsc;
    }

    public void setPriorityOrderAsc(boolean priorityOrderAsc) {
        this.priorityOrderAsc = priorityOrderAsc;
    }

    public boolean isUseSeparateRetryQueue() {
        return useSeparateRetryQueue;
    }

    public void setUseSeparateRetryQueue(boolean useSeparateRetryQueue) {
        this.useSeparateRetryQueue = useSeparateRetryQueue;
    }


    public long getLiteEventCheckInterval() {
        return liteEventCheckInterval;
    }

    public void setLiteEventCheckInterval(long liteEventCheckInterval) {
        this.liteEventCheckInterval = liteEventCheckInterval;
    }

    public long getLiteTtlCheckInterval() {
        return liteTtlCheckInterval;
    }

    public void setLiteTtlCheckInterval(long liteTtlCheckInterval) {
        this.liteTtlCheckInterval = liteTtlCheckInterval;
    }

    public long getMinLiteTTl() {
        return minLiteTTl;
    }

    public void setMinLiteTTl(long minLiteTTl) {
        this.minLiteTTl = minLiteTTl;
    }

    public long getLiteSubscriptionCheckInterval() {
        return liteSubscriptionCheckInterval;
    }

    public void setLiteSubscriptionCheckInterval(long liteSubscriptionCheckInterval) {
        this.liteSubscriptionCheckInterval = liteSubscriptionCheckInterval;
    }

    public long getLiteSubscriptionCheckTimeoutMills() {
        return liteSubscriptionCheckTimeoutMills;
    }

    public void setLiteSubscriptionCheckTimeoutMills(long liteSubscriptionCheckTimeoutMills) {
        this.liteSubscriptionCheckTimeoutMills = liteSubscriptionCheckTimeoutMills;
    }

    public boolean isPersistConsumerOffsetIncrementally() {
        return persistConsumerOffsetIncrementally;
    }

    public void setPersistConsumerOffsetIncrementally(boolean persistConsumerOffsetIncrementally) {
        this.persistConsumerOffsetIncrementally = persistConsumerOffsetIncrementally;
    }

    public long getMaxLiteSubscriptionCount() {
        return maxLiteSubscriptionCount;
    }

    public void setMaxLiteSubscriptionCount(long maxLiteSubscriptionCount) {
        this.maxLiteSubscriptionCount = maxLiteSubscriptionCount;
    }

    public boolean isEnableLitePopLog() {
        return enableLitePopLog;
    }

    public void setEnableLitePopLog(boolean enableLitePopLog) {
        this.enableLitePopLog = enableLitePopLog;
    }

    public int getMaxClientEventCount() {
        return maxClientEventCount;
    }

    public void setMaxClientEventCount(int maxClientEventCount) {
        this.maxClientEventCount = maxClientEventCount;
    }

    public long getLiteEventCapacityCacheTtlMs() {
        return liteEventCapacityCacheTtlMs;
    }

    public void setLiteEventCapacityCacheTtlMs(long liteEventCapacityCacheTtlMs) {
        this.liteEventCapacityCacheTtlMs = liteEventCapacityCacheTtlMs;
    }

    public long getLiteEventFullDispatchDelayTime() {
        return liteEventFullDispatchDelayTime;
    }

    public void setLiteEventFullDispatchDelayTime(long liteEventFullDispatchDelayTime) {
        this.liteEventFullDispatchDelayTime = liteEventFullDispatchDelayTime;
    }

    public long getLiteEventFullDispatchDelayTimeForWildcardGroup() {
        return liteEventFullDispatchDelayTimeForWildcardGroup;
    }

    public void setLiteEventFullDispatchDelayTimeForWildcardGroup(long liteEventFullDispatchDelayTimeForWildcardGroup) {
        this.liteEventFullDispatchDelayTimeForWildcardGroup = liteEventFullDispatchDelayTimeForWildcardGroup;
    }

    public boolean isLiteLagLatencyCollectEnable() {
        return liteLagLatencyCollectEnable;
    }

    public void setLiteLagLatencyCollectEnable(boolean liteLagLatencyCollectEnable) {
        this.liteLagLatencyCollectEnable = liteLagLatencyCollectEnable;
    }

    public boolean isLiteLagLatencyMetricsEnable() {
        return liteLagLatencyMetricsEnable;
    }

    public void setLiteLagLatencyMetricsEnable(boolean liteLagLatencyMetricsEnable) {
        this.liteLagLatencyMetricsEnable = liteLagLatencyMetricsEnable;
    }

    public boolean isLiteLagCountMetricsEnable() {
        return liteLagCountMetricsEnable;
    }

    public void setLiteLagCountMetricsEnable(boolean liteLagCountMetricsEnable) {
        this.liteLagCountMetricsEnable = liteLagCountMetricsEnable;
    }

    public int getLiteLagLatencyTopK() {
        return liteLagLatencyTopK;
    }

    public void setLiteLagLatencyTopK(int liteLagLatencyTopK) {
        this.liteLagLatencyTopK = liteLagLatencyTopK;
    }

    public long getPopOrderLockTimerTickMs() {
        return popOrderLockTimerTickMs;
    }

    public void setPopOrderLockTimerTickMs(long popOrderLockTimerTickMs) {
        this.popOrderLockTimerTickMs = popOrderLockTimerTickMs;
    }

    public int getPopOrderLockTimerTicksPerWheel() {
        return popOrderLockTimerTicksPerWheel;
    }

    public void setPopOrderLockTimerTicksPerWheel(int popOrderLockTimerTicksPerWheel) {
        this.popOrderLockTimerTicksPerWheel = popOrderLockTimerTicksPerWheel;
    }

    public boolean isUseMessageFilterForNotification() {
        return useMessageFilterForNotification;
    }

    public void setUseMessageFilterForNotification(boolean useMessageFilterForNotification) {
        this.useMessageFilterForNotification = useMessageFilterForNotification;
    }

    public int getMaxMessageFilterNumForNotification() {
        return maxMessageFilterNumForNotification;
    }

    public void setMaxMessageFilterNumForNotification(int maxMessageFilterNumForNotification) {
        this.maxMessageFilterNumForNotification = maxMessageFilterNumForNotification;
    }
}
