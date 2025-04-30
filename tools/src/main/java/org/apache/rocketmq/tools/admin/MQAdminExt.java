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
package org.apache.rocketmq.tools.admin;

import org.apache.rocketmq.client.MQAdmin;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.CheckRocksdbCqWriteResult;
import org.apache.rocketmq.common.Pair;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.message.MessageRequestMode;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.exception.RemotingConnectException;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.apache.rocketmq.remoting.exception.RemotingSendRequestException;
import org.apache.rocketmq.remoting.exception.RemotingTimeoutException;
import org.apache.rocketmq.remoting.protocol.admin.ConsumeStats;
import org.apache.rocketmq.remoting.protocol.admin.RollbackStats;
import org.apache.rocketmq.remoting.protocol.admin.TopicStatsTable;
import org.apache.rocketmq.remoting.protocol.body.AclInfo;
import org.apache.rocketmq.remoting.protocol.body.BrokerMemberGroup;
import org.apache.rocketmq.remoting.protocol.body.BrokerReplicasInfo;
import org.apache.rocketmq.remoting.protocol.body.BrokerStatsData;
import org.apache.rocketmq.remoting.protocol.body.ClusterInfo;
import org.apache.rocketmq.remoting.protocol.body.ConsumeMessageDirectlyResult;
import org.apache.rocketmq.remoting.protocol.body.ConsumeStatsList;
import org.apache.rocketmq.remoting.protocol.body.ConsumerConnection;
import org.apache.rocketmq.remoting.protocol.body.ConsumerRunningInfo;
import org.apache.rocketmq.remoting.protocol.body.EpochEntryCache;
import org.apache.rocketmq.remoting.protocol.body.GroupList;
import org.apache.rocketmq.remoting.protocol.body.HARuntimeInfo;
import org.apache.rocketmq.remoting.protocol.body.KVTable;
import org.apache.rocketmq.remoting.protocol.body.MessageRequestModeSerializeWrapper;
import org.apache.rocketmq.remoting.protocol.body.ProducerConnection;
import org.apache.rocketmq.remoting.protocol.body.ProducerTableInfo;
import org.apache.rocketmq.remoting.protocol.body.QueryConsumeQueueResponseBody;
import org.apache.rocketmq.remoting.protocol.body.QueueTimeSpan;
import org.apache.rocketmq.remoting.protocol.body.SubscriptionGroupWrapper;
import org.apache.rocketmq.remoting.protocol.body.TopicConfigSerializeWrapper;
import org.apache.rocketmq.remoting.protocol.body.TopicList;
import org.apache.rocketmq.remoting.protocol.body.UserInfo;
import org.apache.rocketmq.remoting.protocol.header.ExportRocksDBConfigToJsonRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.controller.ElectMasterResponseHeader;
import org.apache.rocketmq.remoting.protocol.header.controller.GetMetaDataResponseHeader;
import org.apache.rocketmq.remoting.protocol.heartbeat.SubscriptionData;
import org.apache.rocketmq.remoting.protocol.route.TopicRouteData;
import org.apache.rocketmq.remoting.protocol.statictopic.TopicQueueMappingDetail;
import org.apache.rocketmq.remoting.protocol.subscription.GroupForbidden;
import org.apache.rocketmq.remoting.protocol.subscription.SubscriptionGroupConfig;
import org.apache.rocketmq.tools.admin.api.BrokerOperatorResult;
import org.apache.rocketmq.tools.admin.api.MessageTrack;
import org.apache.rocketmq.tools.admin.common.AdminToolResult;

import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

public interface MQAdminExt extends MQAdmin {
    void start() throws MQClientException;

    void shutdown();

    void addBrokerToContainer(final String brokerContainerAddr, final String brokerConfig) throws InterruptedException,
        MQBrokerException, RemotingTimeoutException, RemotingSendRequestException, RemotingConnectException;

    void removeBrokerFromContainer(final String brokerContainerAddr, String clusterName, final String brokerName,
        long brokerId) throws InterruptedException, MQBrokerException, RemotingTimeoutException, RemotingSendRequestException, RemotingConnectException;

    void updateBrokerConfig(final String brokerAddr, final Properties properties) throws RemotingConnectException,
        RemotingSendRequestException, RemotingTimeoutException, UnsupportedEncodingException, InterruptedException, MQBrokerException, MQClientException;

    Properties getBrokerConfig(final String brokerAddr) throws RemotingConnectException,
        RemotingSendRequestException, RemotingTimeoutException, UnsupportedEncodingException, InterruptedException, MQBrokerException;

    void createAndUpdateTopicConfig(final String addr,
        final TopicConfig config) throws RemotingException, MQBrokerException,
        InterruptedException, MQClientException;

    void createAndUpdateTopicConfigList(final String addr,
        final List<TopicConfig> topicConfigList) throws InterruptedException, RemotingException, MQClientException;

    void createAndUpdateSubscriptionGroupConfig(final String addr,
        final SubscriptionGroupConfig config) throws RemotingException,
        MQBrokerException, InterruptedException, MQClientException;

    void createAndUpdateSubscriptionGroupConfigList(String brokerAddr,
        List<SubscriptionGroupConfig> configs) throws RemotingException,
        MQBrokerException, InterruptedException, MQClientException;

    SubscriptionGroupConfig examineSubscriptionGroupConfig(final String addr,
        final String group) throws InterruptedException, RemotingException, MQClientException, MQBrokerException;

    TopicStatsTable examineTopicStats(
        final String topic) throws RemotingException, MQClientException, InterruptedException,
        MQBrokerException;

    TopicStatsTable examineTopicStats(String brokerAddr,
        final String topic) throws RemotingException, MQClientException, InterruptedException,
        MQBrokerException;

    AdminToolResult<TopicStatsTable> examineTopicStatsConcurrent(String topic);

    TopicList fetchAllTopicList() throws RemotingException, MQClientException, InterruptedException;

    TopicList fetchTopicsByCLuster(
        String clusterName) throws RemotingException, MQClientException, InterruptedException;

    KVTable fetchBrokerRuntimeStats(
        final String brokerAddr) throws RemotingConnectException, RemotingSendRequestException,
        RemotingTimeoutException, InterruptedException, MQBrokerException;

    ConsumeStats examineConsumeStats(
        final String consumerGroup) throws RemotingException, MQClientException, InterruptedException,
        MQBrokerException;

    CheckRocksdbCqWriteResult checkRocksdbCqWriteProgress(String brokerAddr, String topic, long checkStoreTime)
        throws InterruptedException, RemotingTimeoutException, RemotingSendRequestException, RemotingConnectException, MQClientException;

    ConsumeStats examineConsumeStats(final String consumerGroup,
        final String topic) throws RemotingException, MQClientException,
        InterruptedException, MQBrokerException;

    ConsumeStats examineConsumeStats(final String clusterName, final String consumerGroup,
        final String topic) throws RemotingException, MQClientException,
        InterruptedException, MQBrokerException;

    ConsumeStats examineConsumeStats(final String brokerAddr, final String consumerGroup, final String topicName,
        final long timeoutMillis) throws InterruptedException, RemotingTimeoutException, RemotingSendRequestException,
        RemotingConnectException, MQBrokerException;

    AdminToolResult<ConsumeStats> examineConsumeStatsConcurrent(String consumerGroup, String topic);

    ClusterInfo examineBrokerClusterInfo() throws InterruptedException, MQBrokerException, RemotingTimeoutException,
        RemotingSendRequestException, RemotingConnectException;

    TopicRouteData examineTopicRouteInfo(
        final String topic) throws RemotingException, MQClientException, InterruptedException;

    ConsumerConnection examineConsumerConnectionInfo(final String consumerGroup) throws RemotingConnectException,
        RemotingSendRequestException, RemotingTimeoutException, InterruptedException, MQBrokerException, RemotingException,
        MQClientException;

    ConsumerConnection examineConsumerConnectionInfo(
        String consumerGroup, String brokerAddr) throws InterruptedException, MQBrokerException,
        RemotingException, MQClientException;

    ProducerConnection examineProducerConnectionInfo(final String producerGroup,
        final String topic) throws RemotingException,
        MQClientException, InterruptedException, MQBrokerException;

    ProducerTableInfo getAllProducerInfo(final String brokerAddr) throws RemotingException,
        MQClientException, InterruptedException, MQBrokerException;

    List<String> getNameServerAddressList();

    int wipeWritePermOfBroker(final String namesrvAddr, String brokerName) throws RemotingCommandException,
        RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, InterruptedException, MQClientException;

    int addWritePermOfBroker(final String namesrvAddr, String brokerName) throws RemotingCommandException,
        RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, InterruptedException, MQClientException;

    void putKVConfig(final String namespace, final String key, final String value);

    String getKVConfig(final String namespace,
        final String key) throws RemotingException, MQClientException, InterruptedException;

    KVTable getKVListByNamespace(
        final String namespace) throws RemotingException, MQClientException, InterruptedException;

    void deleteTopic(final String topicName,
        final String clusterName) throws RemotingException, MQBrokerException, InterruptedException, MQClientException;

    void deleteTopicInBroker(final Set<String> addrs, final String topic) throws RemotingException, MQBrokerException,
        InterruptedException, MQClientException;

    AdminToolResult<BrokerOperatorResult> deleteTopicInBrokerConcurrent(Set<String> addrs, String topic);

    void deleteTopicInNameServer(final Set<String> addrs,
        final String topic) throws RemotingException, MQBrokerException,
        InterruptedException, MQClientException;

    void deleteTopicInNameServer(final Set<String> addrs,
        final String clusterName,
        final String topic) throws RemotingException, MQBrokerException,
        InterruptedException, MQClientException;

    void deleteSubscriptionGroup(final String addr, String groupName) throws RemotingException, MQBrokerException,
        InterruptedException, MQClientException;

    void deleteSubscriptionGroup(final String addr, String groupName,
        boolean removeOffset) throws RemotingException, MQBrokerException,
        InterruptedException, MQClientException;

    void createAndUpdateKvConfig(String namespace, String key,
        String value) throws RemotingException, MQBrokerException,
        InterruptedException, MQClientException;

    void deleteKvConfig(String namespace, String key) throws RemotingException, MQBrokerException, InterruptedException,
        MQClientException;

    List<RollbackStats> resetOffsetByTimestampOld(String consumerGroup, String topic, long timestamp, boolean force)
        throws RemotingException, MQBrokerException, InterruptedException, MQClientException;

    Map<MessageQueue, Long> resetOffsetByTimestamp(String topic, String group, long timestamp, boolean isForce)
        throws RemotingException, MQBrokerException, InterruptedException, MQClientException;

    Map<MessageQueue, Long> resetOffsetByTimestamp(String clusterName, String topic, String group, long timestamp, boolean isForce)
        throws RemotingException, MQBrokerException, InterruptedException, MQClientException;

    void resetOffsetNew(String consumerGroup, String topic, long timestamp) throws RemotingException, MQBrokerException,
        InterruptedException, MQClientException;

    AdminToolResult<BrokerOperatorResult> resetOffsetNewConcurrent(final String group, final String topic,
        final long timestamp);

    Map<String, Map<MessageQueue, Long>> getConsumeStatus(String topic, String group,
        String clientAddr) throws RemotingException,
        MQBrokerException, InterruptedException, MQClientException;

    void createOrUpdateOrderConf(String key, String value,
        boolean isCluster) throws RemotingException, MQBrokerException,
        InterruptedException, MQClientException;

    GroupList queryTopicConsumeByWho(final String topic) throws RemotingConnectException, RemotingSendRequestException,
        RemotingTimeoutException, InterruptedException, MQBrokerException, RemotingException, MQClientException;

    TopicList queryTopicsByConsumer(
        final String group) throws InterruptedException, MQBrokerException, RemotingException, MQClientException;

    AdminToolResult<TopicList> queryTopicsByConsumerConcurrent(final String group);

    SubscriptionData querySubscription(final String group,
        final String topic) throws InterruptedException, MQBrokerException, RemotingException, MQClientException;

    List<QueueTimeSpan> queryConsumeTimeSpan(final String topic,
        final String group) throws InterruptedException, MQBrokerException,
        RemotingException, MQClientException;

    AdminToolResult<List<QueueTimeSpan>> queryConsumeTimeSpanConcurrent(final String topic, final String group);

    boolean cleanExpiredConsumerQueue(String cluster) throws RemotingConnectException, RemotingSendRequestException,
        RemotingTimeoutException, MQClientException, InterruptedException;

    boolean cleanExpiredConsumerQueueByAddr(String addr) throws RemotingConnectException, RemotingSendRequestException,
        RemotingTimeoutException, MQClientException, InterruptedException;

    boolean deleteExpiredCommitLog(String cluster) throws RemotingConnectException, RemotingSendRequestException,
        RemotingTimeoutException, MQClientException, InterruptedException;

    boolean deleteExpiredCommitLogByAddr(String addr) throws RemotingConnectException, RemotingSendRequestException,
        RemotingTimeoutException, MQClientException, InterruptedException;

    boolean cleanUnusedTopic(String cluster) throws RemotingConnectException, RemotingSendRequestException,
        RemotingTimeoutException, MQClientException, InterruptedException;

    boolean cleanUnusedTopicByAddr(String addr) throws RemotingConnectException, RemotingSendRequestException,
        RemotingTimeoutException, MQClientException, InterruptedException;

    ConsumerRunningInfo getConsumerRunningInfo(final String consumerGroup, final String clientId, final boolean jstack)
        throws RemotingException, MQClientException, InterruptedException;

    ConsumerRunningInfo getConsumerRunningInfo(final String consumerGroup, final String clientId, final boolean jstack,
        final boolean metrics)
        throws RemotingException, MQClientException, InterruptedException;

    ConsumeMessageDirectlyResult consumeMessageDirectly(String consumerGroup,
        String clientId,
        String topic,
        String msgId) throws RemotingException, MQClientException, InterruptedException, MQBrokerException;

    ConsumeMessageDirectlyResult consumeMessageDirectly(String clusterName, String consumerGroup,
        String clientId,
        String topic,
        String msgId) throws RemotingException, MQClientException, InterruptedException, MQBrokerException;

    List<MessageTrack> messageTrackDetail(
        MessageExt msg) throws RemotingException, MQClientException, InterruptedException,
        MQBrokerException;

    List<MessageTrack> messageTrackDetailConcurrent(
        MessageExt msg) throws RemotingException, MQClientException, InterruptedException,
        MQBrokerException;

    void cloneGroupOffset(String srcGroup, String destGroup, String topic, boolean isOffline) throws RemotingException,
        MQClientException, InterruptedException, MQBrokerException;

    BrokerStatsData viewBrokerStatsData(final String brokerAddr, final String statsName, final String statsKey)
        throws RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, MQClientException,
        InterruptedException;

    Set<String> getClusterList(final String topic) throws RemotingConnectException, RemotingSendRequestException,
        RemotingTimeoutException, MQClientException, InterruptedException;

    ConsumeStatsList fetchConsumeStatsInBroker(final String brokerAddr, boolean isOrder,
        long timeoutMillis) throws RemotingConnectException, RemotingSendRequestException,
        RemotingTimeoutException, MQClientException, InterruptedException;

    Set<String> getTopicClusterList(
        final String topic) throws InterruptedException, MQBrokerException, MQClientException, RemotingException;

    SubscriptionGroupWrapper getAllSubscriptionGroup(final String brokerAddr,
        long timeoutMillis) throws InterruptedException, RemotingTimeoutException, RemotingSendRequestException,
        RemotingConnectException, MQBrokerException;

    SubscriptionGroupWrapper getUserSubscriptionGroup(final String brokerAddr,
        long timeoutMillis) throws InterruptedException, RemotingTimeoutException, RemotingSendRequestException,
        RemotingConnectException, MQBrokerException;

    TopicConfigSerializeWrapper getAllTopicConfig(final String brokerAddr,
        long timeoutMillis) throws InterruptedException, RemotingTimeoutException, RemotingSendRequestException,
        RemotingConnectException, MQBrokerException;

    TopicConfigSerializeWrapper getUserTopicConfig(final String brokerAddr, final boolean specialTopic,
        long timeoutMillis) throws InterruptedException, RemotingException,
        MQBrokerException, MQClientException;

    void updateConsumeOffset(String brokerAddr, String consumeGroup, MessageQueue mq,
        long offset) throws RemotingException, InterruptedException, MQBrokerException;

    /**
     * Update name server config.
     * <br>
     * Command Code : RequestCode.UPDATE_NAMESRV_CONFIG
     *
     * <br> If param(nameServers) is null or empty, will use name servers from ns!
     */
    void updateNameServerConfig(final Properties properties,
        final List<String> nameServers) throws InterruptedException, RemotingConnectException,
        UnsupportedEncodingException, RemotingSendRequestException, RemotingTimeoutException,
        MQClientException, MQBrokerException;

    /**
     * Get name server config.
     * <br>
     * Command Code : RequestCode.GET_NAMESRV_CONFIG
     * <br> If param(nameServers) is null or empty, will use name servers from ns!
     *
     * @return The fetched name server config
     */
    Map<String, Properties> getNameServerConfig(final List<String> nameServers) throws InterruptedException,
        RemotingTimeoutException, RemotingSendRequestException, RemotingConnectException,
        MQClientException, UnsupportedEncodingException;

    /**
     * query consume queue data
     *
     * @param brokerAddr    broker ip address
     * @param topic         topic
     * @param queueId       id of queue
     * @param index         start offset
     * @param count         how many
     * @param consumerGroup group
     */
    QueryConsumeQueueResponseBody queryConsumeQueue(final String brokerAddr,
        final String topic, final int queueId,
        final long index, final int count, final String consumerGroup)
        throws InterruptedException, RemotingTimeoutException, RemotingSendRequestException, RemotingConnectException, MQClientException;

    void exportRocksDBConfigToJson(String brokerAddr,
        List<ExportRocksDBConfigToJsonRequestHeader.ConfigType> configType)
        throws InterruptedException, RemotingTimeoutException, RemotingSendRequestException, RemotingConnectException, MQClientException;

    boolean resumeCheckHalfMessage(final String topic,
        final String msgId) throws RemotingException, MQClientException, InterruptedException, MQBrokerException;

    void setMessageRequestMode(final String brokerAddr, final String topic, final String consumerGroup,
        final MessageRequestMode mode, final int popWorkGroupSize, final long timeoutMillis)
        throws InterruptedException, RemotingTimeoutException, RemotingSendRequestException,
        RemotingConnectException, MQClientException;

    MessageRequestModeSerializeWrapper getAllMessageRequestMode(final String brokerAddr, final long timeoutMillis) throws RemotingConnectException,
        RemotingSendRequestException, RemotingTimeoutException, MQBrokerException, InterruptedException;

    @Deprecated
    long searchOffset(final String brokerAddr, final String topicName,
        final int queueId, final long timestamp, final long timeoutMillis)
        throws RemotingException, MQBrokerException, InterruptedException;

    void resetOffsetByQueueId(final String brokerAddr, final String consumerGroup,
        final String topicName, final int queueId, final long resetOffset)
        throws RemotingException, InterruptedException, MQBrokerException;

    TopicConfig examineTopicConfig(final String addr,
        final String topic) throws InterruptedException, MQBrokerException, RemotingTimeoutException, RemotingSendRequestException, RemotingConnectException;

    void createStaticTopic(final String addr, final String defaultTopic, final TopicConfig topicConfig,
        final TopicQueueMappingDetail mappingDetail,
        final boolean force) throws RemotingException, InterruptedException, MQBrokerException;

    GroupForbidden updateAndGetGroupReadForbidden(String brokerAddr, String groupName, String topicName,
        Boolean readable)
        throws RemotingException, InterruptedException, MQBrokerException;

    MessageExt queryMessage(String clusterName,
        String topic,
        String msgId) throws RemotingException, MQBrokerException, InterruptedException, MQClientException;

    HARuntimeInfo getBrokerHAStatus(String brokerAddr) throws RemotingConnectException, RemotingSendRequestException,
        RemotingTimeoutException, InterruptedException, MQBrokerException;

    BrokerReplicasInfo getInSyncStateData(String controllerAddress,
        List<String> brokers) throws RemotingException, InterruptedException, MQBrokerException;

    EpochEntryCache getBrokerEpochCache(
        String brokerAddr) throws RemotingException, InterruptedException, MQBrokerException;

    GetMetaDataResponseHeader getControllerMetaData(
        String controllerAddr) throws RemotingException, InterruptedException, MQBrokerException;

    /**
     * Reset master flush offset in slave
     *
     * @param brokerAddr        slave broker address
     * @param masterFlushOffset master flush offset
     */
    void resetMasterFlushOffset(String brokerAddr, long masterFlushOffset)
        throws InterruptedException, MQBrokerException, RemotingTimeoutException, RemotingSendRequestException, RemotingConnectException;

    /**
     * Get controller config.
     * <br>
     * Command Code : RequestCode.GET_CONTROLLER_CONFIG
     *
     * @return The fetched controller config
     */
    Map<String, Properties> getControllerConfig(
        List<String> controllerServers) throws InterruptedException, RemotingTimeoutException,
        RemotingSendRequestException, RemotingConnectException, MQClientException, UnsupportedEncodingException;

    /**
     * Update controller config.
     * <br>
     * Command Code : RequestCode.UPDATE_CONTROLLER_CONFIG
     */
    void updateControllerConfig(final Properties properties,
        final List<String> controllers) throws InterruptedException, RemotingConnectException,
        UnsupportedEncodingException, RemotingSendRequestException, RemotingTimeoutException, MQClientException, MQBrokerException;

    /**
     * manual trigger broker elect master
     *
     * @param controllerAddr controller address
     * @param clusterName    cluster name
     * @param brokerName     broker name
     * @param brokerId     broker id
     * @return
     * @throws RemotingException
     * @throws InterruptedException
     * @throws MQBrokerException
     */
    Pair<ElectMasterResponseHeader, BrokerMemberGroup> electMaster(String controllerAddr, String clusterName, String brokerName,
                                                                   Long brokerId) throws RemotingException, InterruptedException, MQBrokerException;

    /**
     * clean controller broker meta data
     */
    void cleanControllerBrokerData(String controllerAddr, String clusterName, String brokerName,
        String brokerControllerIdsToClean,
        boolean isCleanLivingBroker) throws RemotingException, InterruptedException, MQBrokerException;

    void updateColdDataFlowCtrGroupConfig(final String brokerAddr, final Properties properties)
        throws RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, UnsupportedEncodingException, InterruptedException, MQBrokerException;

    void removeColdDataFlowCtrGroupConfig(final String brokerAddr, final String consumerGroup)
        throws RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, UnsupportedEncodingException, InterruptedException, MQBrokerException;

    String getColdDataFlowCtrInfo(final String brokerAddr)
        throws RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, UnsupportedEncodingException, InterruptedException, MQBrokerException;

    String setCommitLogReadAheadMode(final String brokerAddr, String mode)
        throws RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, UnsupportedEncodingException, InterruptedException, MQBrokerException;

    void createUser(String brokerAddr, String username, String password, String userType) throws RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, MQBrokerException, InterruptedException;

    void createUser(String brokerAddr, UserInfo userInfo) throws RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, MQBrokerException, InterruptedException;

    void updateUser(String brokerAddr, String username, String password, String userType, String userStatus) throws RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, MQBrokerException, InterruptedException;

    void updateUser(String brokerAddr, UserInfo userInfo) throws RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, MQBrokerException, InterruptedException;

    void deleteUser(String brokerAddr, String username) throws RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, MQBrokerException, InterruptedException;

    UserInfo getUser(String brokerAddr, String username) throws RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, MQBrokerException, InterruptedException;

    List<UserInfo> listUser(String brokerAddr, String filter) throws RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, MQBrokerException, InterruptedException;

    void createAcl(String brokerAddr, String subject, List<String> resources, List<String> actions, List<String> sourceIps, String decision) throws RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, MQBrokerException, InterruptedException;

    void createAcl(String brokerAddr, AclInfo aclInfo) throws RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, MQBrokerException, InterruptedException;

    void updateAcl(String brokerAddr, String subject, List<String> resources, List<String> actions, List<String> sourceIps, String decision) throws RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, MQBrokerException, InterruptedException;

    void updateAcl(String brokerAddr, AclInfo aclInfo) throws RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, MQBrokerException, InterruptedException;

    void deleteAcl(String brokerAddr, String subject, String resource) throws RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, MQBrokerException, InterruptedException;

    AclInfo getAcl(String brokerAddr, String subject) throws RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, MQBrokerException, InterruptedException;

    List<AclInfo> listAcl(String brokerAddr, String subjectFilter, String resourceFilter) throws RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, MQBrokerException, InterruptedException;

    void exportPopRecords(String brokerAddr, long timeout) throws RemotingConnectException,
        RemotingSendRequestException, RemotingTimeoutException, MQBrokerException, InterruptedException;
}
