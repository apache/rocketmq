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

import org.apache.rocketmq.client.ClientConfig;
import org.apache.rocketmq.client.QueryResult;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.BoundaryType;
import org.apache.rocketmq.common.CheckRocksdbCqWriteResult;
import org.apache.rocketmq.common.Pair;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.message.MessageRequestMode;
import org.apache.rocketmq.common.topic.TopicValidator;
import org.apache.rocketmq.remoting.RPCHook;
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

public class DefaultMQAdminExt extends ClientConfig implements MQAdminExt {
    private final DefaultMQAdminExtImpl defaultMQAdminExtImpl;
    private String adminExtGroup = "admin_ext_group";
    private String createTopicKey = TopicValidator.AUTO_CREATE_TOPIC_KEY_TOPIC;
    private long timeoutMillis = 5000;

    public DefaultMQAdminExt() {
        this.defaultMQAdminExtImpl = new DefaultMQAdminExtImpl(this, null, timeoutMillis);
    }

    public DefaultMQAdminExt(long timeoutMillis) {
        this.defaultMQAdminExtImpl = new DefaultMQAdminExtImpl(this, null, timeoutMillis);
    }

    public DefaultMQAdminExt(RPCHook rpcHook) {
        this.defaultMQAdminExtImpl = new DefaultMQAdminExtImpl(this, rpcHook, timeoutMillis);
    }

    public DefaultMQAdminExt(RPCHook rpcHook, long timeoutMillis) {
        this.defaultMQAdminExtImpl = new DefaultMQAdminExtImpl(this, rpcHook, timeoutMillis);
    }

    public DefaultMQAdminExt(final String adminExtGroup) {
        this.adminExtGroup = adminExtGroup;
        this.defaultMQAdminExtImpl = new DefaultMQAdminExtImpl(this, timeoutMillis);
    }

    public DefaultMQAdminExt(final String adminExtGroup, long timeoutMillis) {
        this.adminExtGroup = adminExtGroup;
        this.defaultMQAdminExtImpl = new DefaultMQAdminExtImpl(this, timeoutMillis);
    }

    @Override
    public void createTopic(String key, String newTopic, int queueNum,
        Map<String, String> attributes) throws MQClientException {
        createTopic(key, newTopic, queueNum, 0, attributes);
    }

    @Override
    public void createTopic(String key, String newTopic, int queueNum, int topicSysFlag,
        Map<String, String> attributes) throws MQClientException {
        defaultMQAdminExtImpl.createTopic(key, newTopic, queueNum, topicSysFlag, attributes);
    }

    @Override
    public long searchOffset(MessageQueue mq, long timestamp) throws MQClientException {
        return defaultMQAdminExtImpl.searchOffset(mq, timestamp);
    }

    public long searchLowerBoundaryOffset(MessageQueue mq, long timestamp) throws MQClientException {
        return defaultMQAdminExtImpl.searchOffset(mq, timestamp, BoundaryType.LOWER);
    }

    public long searchUpperBoundaryOffset(MessageQueue mq, long timestamp) throws MQClientException {
        return defaultMQAdminExtImpl.searchOffset(mq, timestamp, BoundaryType.UPPER);
    }

    @Override
    public long maxOffset(MessageQueue mq) throws MQClientException {
        return defaultMQAdminExtImpl.maxOffset(mq);
    }

    @Override
    public long minOffset(MessageQueue mq) throws MQClientException {
        return defaultMQAdminExtImpl.minOffset(mq);
    }

    @Override
    public long earliestMsgStoreTime(MessageQueue mq) throws MQClientException {
        return defaultMQAdminExtImpl.earliestMsgStoreTime(mq);
    }

    @Override
    public QueryResult queryMessage(String topic, String key, int maxNum, long begin, long end)
        throws MQClientException, InterruptedException {
        return defaultMQAdminExtImpl.queryMessage(topic, key, maxNum, begin, end);
    }


    public QueryResult queryMessage(String clusterName, String topic, String key, int maxNum, long begin, long end)
        throws MQClientException, InterruptedException, RemotingException {
        return defaultMQAdminExtImpl.queryMessage(clusterName, topic, key, maxNum, begin, end);
    }

    @Override
    public void start() throws MQClientException {
        defaultMQAdminExtImpl.start();
    }

    @Override
    public void shutdown() {
        defaultMQAdminExtImpl.shutdown();
    }

    @Override
    public void addBrokerToContainer(String brokerContainerAddr, String brokerConfig) throws InterruptedException,
        RemotingConnectException, RemotingTimeoutException, RemotingSendRequestException, MQBrokerException {
        defaultMQAdminExtImpl.addBrokerToContainer(brokerContainerAddr, brokerConfig);
    }

    @Override
    public void removeBrokerFromContainer(String brokerContainerAddr, String clusterName, String brokerName,
        long brokerId) throws InterruptedException, MQBrokerException, RemotingTimeoutException, RemotingSendRequestException,
        RemotingConnectException {
        defaultMQAdminExtImpl.removeBrokerFromContainer(brokerContainerAddr, clusterName, brokerName, brokerId);
    }

    @Override
    public void updateBrokerConfig(String brokerAddr,
        Properties properties) throws RemotingConnectException, RemotingSendRequestException,
        RemotingTimeoutException, UnsupportedEncodingException, InterruptedException, MQBrokerException, MQClientException {
        defaultMQAdminExtImpl.updateBrokerConfig(brokerAddr, properties);
    }

    @Override
    public Properties getBrokerConfig(final String brokerAddr) throws RemotingConnectException,
        RemotingSendRequestException, RemotingTimeoutException, UnsupportedEncodingException, InterruptedException, MQBrokerException {
        return defaultMQAdminExtImpl.getBrokerConfig(brokerAddr);
    }

    @Override
    public void createAndUpdateTopicConfig(String addr, TopicConfig config) throws RemotingException, MQBrokerException,
        InterruptedException, MQClientException {
        defaultMQAdminExtImpl.createAndUpdateTopicConfig(addr, config);
    }

    @Override
    public void createAndUpdateTopicConfigList(String addr,
        List<TopicConfig> topicConfigList) throws InterruptedException, RemotingException, MQClientException {
        defaultMQAdminExtImpl.createAndUpdateTopicConfigList(addr, topicConfigList);
    }

    @Override
    public void createAndUpdateSubscriptionGroupConfig(String addr,
        SubscriptionGroupConfig config) throws RemotingException,
        MQBrokerException, InterruptedException, MQClientException {
        defaultMQAdminExtImpl.createAndUpdateSubscriptionGroupConfig(addr, config);
    }

    @Override
    public void createAndUpdateSubscriptionGroupConfigList(String brokerAddr,
        List<SubscriptionGroupConfig> configs) throws RemotingException, MQBrokerException, InterruptedException, MQClientException {
        defaultMQAdminExtImpl.createAndUpdateSubscriptionGroupConfigList(brokerAddr, configs);
    }

    @Override
    public SubscriptionGroupConfig examineSubscriptionGroupConfig(String addr, String group)
        throws InterruptedException, RemotingException, MQClientException, MQBrokerException {
        return defaultMQAdminExtImpl.examineSubscriptionGroupConfig(addr, group);
    }

    @Override
    public TopicConfig examineTopicConfig(String addr,
        String topic) throws RemotingSendRequestException, RemotingConnectException, RemotingTimeoutException, InterruptedException, MQBrokerException {
        return defaultMQAdminExtImpl.examineTopicConfig(addr, topic);
    }

    @Override
    public TopicStatsTable examineTopicStats(
        String topic) throws RemotingException, MQClientException, InterruptedException,
        MQBrokerException {
        return defaultMQAdminExtImpl.examineTopicStats(topic);
    }

    @Override
    public TopicStatsTable examineTopicStats(String brokerAddr,
        String topic) throws RemotingException, MQClientException, InterruptedException,
        MQBrokerException {
        return defaultMQAdminExtImpl.examineTopicStats(brokerAddr, topic);
    }

    @Override
    public AdminToolResult<TopicStatsTable> examineTopicStatsConcurrent(String topic) {
        return defaultMQAdminExtImpl.examineTopicStatsConcurrent(topic);
    }

    @Override
    public TopicList fetchAllTopicList() throws RemotingException, MQClientException, InterruptedException {
        return this.defaultMQAdminExtImpl.fetchAllTopicList();
    }
    
    @Override
    public TopicList fetchAllRetryTopicList() throws RemotingException, MQClientException, InterruptedException {
        return this.defaultMQAdminExtImpl.fetchAllRetryTopicList();
    }

    @Override
    public TopicList fetchTopicsByCLuster(
        String clusterName) throws RemotingException, MQClientException, InterruptedException {
        return this.defaultMQAdminExtImpl.fetchTopicsByCLuster(clusterName);
    }

    @Override
    public KVTable fetchBrokerRuntimeStats(
        final String brokerAddr) throws RemotingConnectException, RemotingSendRequestException,
        RemotingTimeoutException, InterruptedException, MQBrokerException {
        return this.defaultMQAdminExtImpl.fetchBrokerRuntimeStats(brokerAddr);
    }

    @Override
    public ConsumeStats examineConsumeStats(
        String consumerGroup) throws RemotingException, MQClientException, InterruptedException,
        MQBrokerException {
        return examineConsumeStats(consumerGroup, null);
    }

    @Override
    public ConsumeStats examineConsumeStats(String clusterName, String consumerGroup,
        String topic) throws RemotingException, MQClientException, InterruptedException, MQBrokerException {
        return defaultMQAdminExtImpl.examineConsumeStats(clusterName, consumerGroup, topic);
    }

    @Override
    public ConsumeStats examineConsumeStats(String consumerGroup,
        String topic) throws RemotingException, MQClientException,
        InterruptedException, MQBrokerException {
        return defaultMQAdminExtImpl.examineConsumeStats(consumerGroup, topic);
    }

    @Override
    public ConsumeStats examineConsumeStats(final String brokerAddr, final String consumerGroup,
        final String topicName, final long timeoutMillis)
        throws InterruptedException, RemotingTimeoutException, RemotingSendRequestException, RemotingConnectException, MQBrokerException {
        return this.defaultMQAdminExtImpl.examineConsumeStats(brokerAddr, consumerGroup, topicName, timeoutMillis);
    }

    @Override
    public AdminToolResult<ConsumeStats> examineConsumeStatsConcurrent(String consumerGroup, String topic) {
        return defaultMQAdminExtImpl.examineConsumeStatsConcurrent(consumerGroup, topic);
    }

    @Override
    public ClusterInfo examineBrokerClusterInfo() throws InterruptedException, RemotingConnectException, RemotingTimeoutException,
        RemotingSendRequestException, MQBrokerException {
        return defaultMQAdminExtImpl.examineBrokerClusterInfo();
    }

    @Override
    public TopicRouteData examineTopicRouteInfo(
        String topic) throws RemotingException, MQClientException, InterruptedException {
        return defaultMQAdminExtImpl.examineTopicRouteInfo(topic);
    }

    @Override
    public ConsumerConnection examineConsumerConnectionInfo(
        String consumerGroup) throws InterruptedException, MQBrokerException,
        RemotingException, MQClientException {
        return defaultMQAdminExtImpl.examineConsumerConnectionInfo(consumerGroup);
    }

    @Override
    public ConsumerConnection examineConsumerConnectionInfo(
        String consumerGroup, String brokerAddr) throws InterruptedException, MQBrokerException,
        RemotingException, MQClientException {
        return defaultMQAdminExtImpl.examineConsumerConnectionInfo(consumerGroup, brokerAddr);
    }

    @Override
    public ProducerConnection examineProducerConnectionInfo(String producerGroup,
        final String topic) throws RemotingException,
        MQClientException, InterruptedException, MQBrokerException {
        return defaultMQAdminExtImpl.examineProducerConnectionInfo(producerGroup, topic);
    }

    @Override
    public ProducerTableInfo getAllProducerInfo(
        final String brokerAddr) throws RemotingException, MQClientException, InterruptedException, MQBrokerException {
        return defaultMQAdminExtImpl.getAllProducerInfo(brokerAddr);
    }

    @Override
    public void deleteTopicInNameServer(Set<String> addrs, String clusterName,
        String topic) throws RemotingException, MQBrokerException, InterruptedException, MQClientException {
        defaultMQAdminExtImpl.deleteTopicInNameServer(addrs, clusterName, topic);
    }

    @Override
    public List<String> getNameServerAddressList() {
        return this.defaultMQAdminExtImpl.getNameServerAddressList();
    }

    @Override
    public int wipeWritePermOfBroker(final String namesrvAddr, String brokerName) throws RemotingCommandException,
        RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, InterruptedException, MQClientException {
        return defaultMQAdminExtImpl.wipeWritePermOfBroker(namesrvAddr, brokerName);
    }

    @Override
    public int addWritePermOfBroker(String namesrvAddr,
        String brokerName) throws RemotingCommandException, RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, InterruptedException, MQClientException {
        return defaultMQAdminExtImpl.addWritePermOfBroker(namesrvAddr, brokerName);
    }

    @Override
    public void putKVConfig(String namespace, String key, String value) {
        defaultMQAdminExtImpl.putKVConfig(namespace, key, value);
    }

    @Override
    public String getKVConfig(String namespace,
        String key) throws RemotingException, MQClientException, InterruptedException {
        return defaultMQAdminExtImpl.getKVConfig(namespace, key);
    }

    @Override
    public KVTable getKVListByNamespace(
        String namespace) throws RemotingException, MQClientException, InterruptedException {
        return defaultMQAdminExtImpl.getKVListByNamespace(namespace);
    }

    @Override
    public void deleteTopic(String topicName,
        String clusterName) throws RemotingException, MQBrokerException, InterruptedException,
        MQClientException {
        defaultMQAdminExtImpl.deleteTopic(topicName, clusterName);
    }

    @Override
    public void deleteTopicInBroker(Set<String> addrs,
        String topic) throws RemotingException, MQBrokerException, InterruptedException,
        MQClientException {
        defaultMQAdminExtImpl.deleteTopicInBroker(addrs, topic);
    }

    @Override
    public AdminToolResult<BrokerOperatorResult> deleteTopicInBrokerConcurrent(Set<String> addrs, String topic) {
        return defaultMQAdminExtImpl.deleteTopicInBrokerConcurrent(addrs, topic);
    }

    @Override
    public void deleteTopicInNameServer(Set<String> addrs,
        String topic) throws RemotingException, MQBrokerException, InterruptedException,
        MQClientException {
        defaultMQAdminExtImpl.deleteTopicInNameServer(addrs, topic);
    }

    @Override
    public void deleteSubscriptionGroup(String addr,
        String groupName) throws RemotingException, MQBrokerException, InterruptedException,
        MQClientException {
        defaultMQAdminExtImpl.deleteSubscriptionGroup(addr, groupName);
    }

    @Override
    public void deleteSubscriptionGroup(String addr,
        String groupName, boolean removeOffset) throws RemotingException, MQBrokerException, InterruptedException,
        MQClientException {
        defaultMQAdminExtImpl.deleteSubscriptionGroup(addr, groupName, removeOffset);
    }

    @Override
    public void createAndUpdateKvConfig(String namespace, String key,
        String value) throws RemotingException, MQBrokerException,
        InterruptedException, MQClientException {
        defaultMQAdminExtImpl.createAndUpdateKvConfig(namespace, key, value);
    }

    @Override
    public void deleteKvConfig(String namespace,
        String key) throws RemotingException, MQBrokerException, InterruptedException,
        MQClientException {
        defaultMQAdminExtImpl.deleteKvConfig(namespace, key);
    }

    @Override
    public List<RollbackStats> resetOffsetByTimestampOld(String consumerGroup, String topic, long timestamp,
        boolean force)
        throws RemotingException, MQBrokerException, InterruptedException, MQClientException {
        return defaultMQAdminExtImpl.resetOffsetByTimestampOld(consumerGroup, topic, timestamp, force);
    }

    public List<RollbackStats> resetOffsetByTimestampOld(String clusterName, String consumerGroup, String topic, long timestamp,
        boolean force)
        throws RemotingException, MQBrokerException, InterruptedException, MQClientException {
        return defaultMQAdminExtImpl.resetOffsetByTimestampOld(clusterName, consumerGroup, topic, timestamp, force);
    }

    @Override
    public Map<MessageQueue, Long> resetOffsetByTimestamp(String clusterName, String topic, String group,
        long timestamp, boolean isForce) throws RemotingException, MQBrokerException, InterruptedException, MQClientException {
        return defaultMQAdminExtImpl.resetOffsetByTimestamp(clusterName, topic, group, timestamp, isForce);
    }

    @Override
    public Map<MessageQueue, Long> resetOffsetByTimestamp(String topic, String group, long timestamp, boolean isForce)
        throws RemotingException, MQBrokerException, InterruptedException, MQClientException {
        return resetOffsetByTimestamp(topic, group, timestamp, isForce, false);
    }

    public Map<MessageQueue, Long> resetOffsetByTimestamp(String clusterName, String topic, String group,
        long timestamp, boolean isForce, boolean isC)
        throws RemotingException, MQBrokerException, InterruptedException, MQClientException {
        return defaultMQAdminExtImpl.resetOffsetByTimestamp(clusterName, topic, group, timestamp, isForce, isC);
    }


    public Map<MessageQueue, Long> resetOffsetByTimestamp(String topic, String group, long timestamp, boolean isForce,
        boolean isC)
        throws RemotingException, MQBrokerException, InterruptedException, MQClientException {
        return defaultMQAdminExtImpl.resetOffsetByTimestamp(null, topic, group, timestamp, isForce, isC);
    }

    @Override
    public void resetOffsetNew(String consumerGroup, String topic,
        long timestamp) throws RemotingException, MQBrokerException,
        InterruptedException, MQClientException {
        this.defaultMQAdminExtImpl.resetOffsetNew(consumerGroup, topic, timestamp);
    }

    @Override
    public AdminToolResult<BrokerOperatorResult> resetOffsetNewConcurrent(final String group, final String topic,
        final long timestamp) {
        return this.defaultMQAdminExtImpl.resetOffsetNewConcurrent(group, topic, timestamp);
    }

    @Override
    public Map<String, Map<MessageQueue, Long>> getConsumeStatus(String topic, String group,
        String clientAddr) throws RemotingException,
        MQBrokerException, InterruptedException, MQClientException {
        return defaultMQAdminExtImpl.getConsumeStatus(topic, group, clientAddr);
    }

    @Override
    public void createOrUpdateOrderConf(String key, String value,
        boolean isCluster) throws RemotingException, MQBrokerException,
        InterruptedException, MQClientException {
        defaultMQAdminExtImpl.createOrUpdateOrderConf(key, value, isCluster);
    }

    @Override
    public GroupList queryTopicConsumeByWho(
        String topic) throws InterruptedException, MQBrokerException, RemotingException,
        MQClientException {
        return this.defaultMQAdminExtImpl.queryTopicConsumeByWho(topic);
    }

    @Override
    public TopicList queryTopicsByConsumer(String group)
        throws InterruptedException, MQBrokerException, RemotingException, MQClientException {
        return this.defaultMQAdminExtImpl.queryTopicsByConsumer(group);
    }

    @Override
    public AdminToolResult<TopicList> queryTopicsByConsumerConcurrent(String group) {
        return defaultMQAdminExtImpl.queryTopicsByConsumerConcurrent(group);
    }

    @Override
    public SubscriptionData querySubscription(String group, String topic)
        throws InterruptedException, MQBrokerException, RemotingException, MQClientException {
        return this.defaultMQAdminExtImpl.querySubscription(group, topic);
    }

    @Override
    public List<QueueTimeSpan> queryConsumeTimeSpan(final String topic,
        final String group) throws InterruptedException, MQBrokerException,
        RemotingException, MQClientException {
        return this.defaultMQAdminExtImpl.queryConsumeTimeSpan(topic, group);
    }

    @Override
    public AdminToolResult<List<QueueTimeSpan>> queryConsumeTimeSpanConcurrent(String topic, String group) {
        return defaultMQAdminExtImpl.queryConsumeTimeSpanConcurrent(topic, group);
    }

    @Override
    public boolean cleanExpiredConsumerQueue(
        String cluster) throws RemotingConnectException, RemotingSendRequestException,
        RemotingTimeoutException, MQClientException, InterruptedException {
        return defaultMQAdminExtImpl.cleanExpiredConsumerQueue(cluster);
    }

    @Override
    public boolean cleanExpiredConsumerQueueByAddr(
        String addr) throws RemotingConnectException, RemotingSendRequestException,
        RemotingTimeoutException, MQClientException, InterruptedException {
        return defaultMQAdminExtImpl.cleanExpiredConsumerQueueByAddr(addr);
    }

    @Override
    public boolean deleteExpiredCommitLog(String cluster) throws RemotingConnectException, RemotingSendRequestException,
        RemotingTimeoutException, MQClientException, InterruptedException {
        return defaultMQAdminExtImpl.deleteExpiredCommitLog(cluster);
    }

    @Override
    public boolean deleteExpiredCommitLogByAddr(
        String addr) throws RemotingConnectException, RemotingSendRequestException,
        RemotingTimeoutException, MQClientException, InterruptedException {
        return defaultMQAdminExtImpl.deleteExpiredCommitLogByAddr(addr);
    }

    @Override
    public boolean cleanUnusedTopic(String cluster) throws RemotingConnectException, RemotingSendRequestException,
        RemotingTimeoutException, MQClientException, InterruptedException {
        return defaultMQAdminExtImpl.cleanUnusedTopic(cluster);
    }

    @Override
    public boolean cleanUnusedTopicByAddr(String addr) throws RemotingConnectException, RemotingSendRequestException,
        RemotingTimeoutException, MQClientException, InterruptedException {
        return defaultMQAdminExtImpl.cleanUnusedTopicByAddr(addr);
    }

    @Override
    public ConsumerRunningInfo getConsumerRunningInfo(String consumerGroup, String clientId,
        boolean jstack) throws RemotingException,
        MQClientException, InterruptedException {
        return defaultMQAdminExtImpl.getConsumerRunningInfo(consumerGroup, clientId, jstack);
    }

    @Override
    public ConsumerRunningInfo getConsumerRunningInfo(String consumerGroup, String clientId,
        boolean jstack, boolean metrics) throws RemotingException,
        MQClientException, InterruptedException {
        return defaultMQAdminExtImpl.getConsumerRunningInfo(consumerGroup, clientId, jstack, metrics);
    }

    @Override
    public ConsumeMessageDirectlyResult consumeMessageDirectly(final String consumerGroup, final String clientId,
        final String topic,
        final String msgId) throws RemotingException, MQClientException, InterruptedException, MQBrokerException {
        return defaultMQAdminExtImpl.consumeMessageDirectly(consumerGroup, clientId, topic, msgId);
    }

    @Override
    public ConsumeMessageDirectlyResult consumeMessageDirectly(final String clusterName, final String consumerGroup,
        final String clientId,
        final String topic,
        final String msgId) throws RemotingException, MQClientException, InterruptedException, MQBrokerException {
        return defaultMQAdminExtImpl.consumeMessageDirectly(clusterName, consumerGroup, clientId, topic, msgId);
    }

    @Override
    public List<MessageTrack> messageTrackDetail(
        MessageExt msg) throws RemotingException, MQClientException, InterruptedException,
        MQBrokerException {
        return this.defaultMQAdminExtImpl.messageTrackDetail(msg);
    }

    @Override
    public List<MessageTrack> messageTrackDetailConcurrent(
        MessageExt msg) throws RemotingException, MQClientException, InterruptedException,
        MQBrokerException {
        return this.defaultMQAdminExtImpl.messageTrackDetailConcurrent(msg);
    }

    @Override
    public void cloneGroupOffset(String srcGroup, String destGroup, String topic,
        boolean isOffline) throws RemotingException,
        MQClientException, InterruptedException, MQBrokerException {
        this.defaultMQAdminExtImpl.cloneGroupOffset(srcGroup, destGroup, topic, isOffline);
    }

    @Override
    public BrokerStatsData viewBrokerStatsData(String brokerAddr, String statsName,
        String statsKey) throws RemotingConnectException,
        RemotingSendRequestException, RemotingTimeoutException, MQClientException, InterruptedException {
        return this.defaultMQAdminExtImpl.viewBrokerStatsData(brokerAddr, statsName, statsKey);
    }

    @Override
    public Set<String> getClusterList(String topic) throws RemotingConnectException, RemotingSendRequestException,
        RemotingTimeoutException, MQClientException, InterruptedException {
        return this.defaultMQAdminExtImpl.getClusterList(topic);
    }

    @Override
    public ConsumeStatsList fetchConsumeStatsInBroker(final String brokerAddr, boolean isOrder,
        long timeoutMillis) throws RemotingConnectException, RemotingSendRequestException,
        RemotingTimeoutException, MQClientException, InterruptedException {
        return this.defaultMQAdminExtImpl.fetchConsumeStatsInBroker(brokerAddr, isOrder, timeoutMillis);
    }

    @Override
    public Set<String> getTopicClusterList(
        final String topic) throws InterruptedException, MQBrokerException, MQClientException, RemotingException {
        return this.defaultMQAdminExtImpl.getTopicClusterList(topic);
    }

    @Override
    public SubscriptionGroupWrapper getAllSubscriptionGroup(final String brokerAddr,
        long timeoutMillis) throws InterruptedException, RemotingTimeoutException, RemotingSendRequestException,
        RemotingConnectException, MQBrokerException {
        return this.defaultMQAdminExtImpl.getAllSubscriptionGroup(brokerAddr, timeoutMillis);
    }

    @Override
    public SubscriptionGroupWrapper getUserSubscriptionGroup(final String brokerAddr,
        long timeoutMillis) throws InterruptedException, RemotingTimeoutException, RemotingSendRequestException,
        RemotingConnectException, MQBrokerException {
        return this.defaultMQAdminExtImpl.getUserSubscriptionGroup(brokerAddr, timeoutMillis);
    }

    @Override
    public TopicConfigSerializeWrapper getAllTopicConfig(final String brokerAddr,
        long timeoutMillis) throws InterruptedException, RemotingTimeoutException, RemotingSendRequestException,
        RemotingConnectException, MQBrokerException {
        return this.defaultMQAdminExtImpl.getAllTopicConfig(brokerAddr, timeoutMillis);
    }

    @Override
    public TopicConfigSerializeWrapper getUserTopicConfig(final String brokerAddr, final boolean specialTopic,
        long timeoutMillis) throws InterruptedException, RemotingException,
        MQBrokerException, MQClientException {
        return this.defaultMQAdminExtImpl.getUserTopicConfig(brokerAddr, specialTopic, timeoutMillis);
    }

    /* (non-Javadoc)
     * @see org.apache.rocketmq.client.MQAdmin#queryMessageByUniqKey(java.lang.String, java.lang.String)
     */
    @Override
    public MessageExt viewMessage(String topic, String msgId)
        throws RemotingException, MQBrokerException, InterruptedException, MQClientException {
        return this.defaultMQAdminExtImpl.viewMessage(topic, msgId);
    }

    @Override
    public MessageExt queryMessage(String clusterName, String topic, String msgId)
        throws RemotingException, MQBrokerException, InterruptedException, MQClientException {
        return this.defaultMQAdminExtImpl.queryMessage(clusterName, topic, msgId);
    }

    public String getAdminExtGroup() {
        return adminExtGroup;
    }

    public void setAdminExtGroup(String adminExtGroup) {
        this.adminExtGroup = adminExtGroup;
    }

    public String getCreateTopicKey() {
        return createTopicKey;
    }

    public void setCreateTopicKey(String createTopicKey) {
        this.createTopicKey = createTopicKey;
    }

    @Override
    public void updateConsumeOffset(String brokerAddr, String consumeGroup, MessageQueue mq,
        long offset) throws RemotingException, InterruptedException, MQBrokerException {
        this.defaultMQAdminExtImpl.updateConsumeOffset(brokerAddr, consumeGroup, mq, offset);
    }

    @Override
    public void updateNameServerConfig(final Properties properties, final List<String> nameServers)
        throws InterruptedException, RemotingConnectException,
        UnsupportedEncodingException, MQBrokerException, RemotingTimeoutException,
        MQClientException, RemotingSendRequestException {
        this.defaultMQAdminExtImpl.updateNameServerConfig(properties, nameServers);
    }

    @Override
    public Map<String, Properties> getNameServerConfig(final List<String> nameServers)
        throws InterruptedException, RemotingTimeoutException,
        RemotingSendRequestException, RemotingConnectException, MQClientException,
        UnsupportedEncodingException {
        return this.defaultMQAdminExtImpl.getNameServerConfig(nameServers);
    }

    @Override
    public QueryConsumeQueueResponseBody queryConsumeQueue(String brokerAddr, String topic, int queueId, long index,
        int count, String consumerGroup)
        throws InterruptedException, RemotingTimeoutException, RemotingSendRequestException, RemotingConnectException, MQClientException {
        return this.defaultMQAdminExtImpl.queryConsumeQueue(
            brokerAddr, topic, queueId, index, count, consumerGroup
        );
    }

    @Override
    public CheckRocksdbCqWriteResult checkRocksdbCqWriteProgress(String brokerAddr, String topic, long checkStoreTime)
        throws InterruptedException, RemotingTimeoutException, RemotingSendRequestException, RemotingConnectException, MQClientException {
        return this.defaultMQAdminExtImpl.checkRocksdbCqWriteProgress(brokerAddr, topic, checkStoreTime);
    }

    @Override
    public void exportRocksDBConfigToJson(String brokerAddr,
        List<ExportRocksDBConfigToJsonRequestHeader.ConfigType> configType)
        throws InterruptedException, RemotingTimeoutException, RemotingSendRequestException, RemotingConnectException, MQClientException {
        this.defaultMQAdminExtImpl.exportRocksDBConfigToJson(brokerAddr, configType);
    }

    @Override
    public boolean resumeCheckHalfMessage(String topic,
        String msgId)
        throws RemotingException, MQClientException, InterruptedException, MQBrokerException {
        return this.defaultMQAdminExtImpl.resumeCheckHalfMessage(topic, msgId);
    }

    @Override
    public void setMessageRequestMode(final String brokerAddr, final String topic, final String consumerGroup,
        final MessageRequestMode mode, final int popShareQueueNum, final long timeoutMillis)
        throws InterruptedException, RemotingTimeoutException, RemotingSendRequestException,
        RemotingConnectException, MQClientException {
        this.defaultMQAdminExtImpl.setMessageRequestMode(brokerAddr, topic, consumerGroup, mode, popShareQueueNum, timeoutMillis);
    }

    @Override
    public void createStaticTopic(String addr, String defaultTopic, TopicConfig topicConfig,
        TopicQueueMappingDetail mappingDetail,
        boolean force) throws RemotingException, InterruptedException, MQBrokerException {
        this.defaultMQAdminExtImpl.createStaticTopic(addr, defaultTopic, topicConfig, mappingDetail, force);
    }

    @Deprecated
    @Override
    public long searchOffset(final String brokerAddr, final String topicName,
        final int queueId, final long timestamp, final long timeoutMillis)
        throws RemotingException, MQBrokerException, InterruptedException {
        return this.defaultMQAdminExtImpl.searchOffset(brokerAddr, topicName, queueId, timestamp, timeoutMillis);
    }

    @Override
    public void resetOffsetByQueueId(final String brokerAddr, final String consumerGroup,
        final String topicName, final int queueId, final long resetOffset)
        throws RemotingException, InterruptedException, MQBrokerException {
        this.defaultMQAdminExtImpl.resetOffsetByQueueId(brokerAddr, consumerGroup, topicName, queueId, resetOffset);
    }

    @Override
    public HARuntimeInfo getBrokerHAStatus(String brokerAddr) throws RemotingConnectException,
        RemotingSendRequestException, RemotingTimeoutException, InterruptedException, MQBrokerException {
        return this.defaultMQAdminExtImpl.getBrokerHAStatus(brokerAddr);
    }

    @Override
    public BrokerReplicasInfo getInSyncStateData(String controllerAddress,
        List<String> brokers) throws RemotingException, InterruptedException, MQBrokerException {
        return this.defaultMQAdminExtImpl.getInSyncStateData(controllerAddress, brokers);
    }

    @Override
    public EpochEntryCache getBrokerEpochCache(
        String brokerAddr) throws RemotingException, InterruptedException, MQBrokerException {
        return this.defaultMQAdminExtImpl.getBrokerEpochCache(brokerAddr);
    }

    public GetMetaDataResponseHeader getControllerMetaData(
        String controllerAddr) throws RemotingException, InterruptedException, MQBrokerException {
        return this.defaultMQAdminExtImpl.getControllerMetaData(controllerAddr);
    }

    @Override
    public void resetMasterFlushOffset(String brokerAddr, long masterFlushOffset)
        throws InterruptedException, RemotingConnectException, RemotingTimeoutException, RemotingSendRequestException, MQBrokerException {
        this.defaultMQAdminExtImpl.resetMasterFlushOffset(brokerAddr, masterFlushOffset);
    }

    public QueryResult queryMessageByUniqKey(String clusterName, String topic, String key, int maxNum, long begin,
        long end)
        throws MQClientException, InterruptedException {
        return defaultMQAdminExtImpl.queryMessageByUniqKey(clusterName, topic, key, maxNum, begin, end);
    }

    public DefaultMQAdminExtImpl getDefaultMQAdminExtImpl() {
        return defaultMQAdminExtImpl;
    }

    public GroupForbidden updateAndGetGroupReadForbidden(String brokerAddr, //
        String groupName, //
        String topicName, //
        Boolean readable) //
        throws RemotingException,
        InterruptedException, MQBrokerException {
        return this.defaultMQAdminExtImpl.updateAndGetGroupReadForbidden(brokerAddr, groupName, topicName, readable);
    }

    @Override
    public Map<String, Properties> getControllerConfig(
        List<String> controllerServers) throws InterruptedException, RemotingTimeoutException,
        RemotingSendRequestException, RemotingConnectException, MQClientException,
        UnsupportedEncodingException {
        return this.defaultMQAdminExtImpl.getControllerConfig(controllerServers);
    }

    @Override
    public void updateControllerConfig(Properties properties,
        List<String> controllers) throws InterruptedException, RemotingConnectException, UnsupportedEncodingException, RemotingSendRequestException, RemotingTimeoutException, MQClientException, MQBrokerException {
        this.defaultMQAdminExtImpl.updateControllerConfig(properties, controllers);
    }

    @Override
    public Pair<ElectMasterResponseHeader, BrokerMemberGroup> electMaster(String controllerAddr, String clusterName,
        String brokerName, Long brokerId) throws RemotingException, InterruptedException, MQBrokerException {
        return this.defaultMQAdminExtImpl.electMaster(controllerAddr, clusterName, brokerName, brokerId);
    }

    @Override
    public void cleanControllerBrokerData(String controllerAddr, String clusterName, String brokerName,
        String brokerControllerIdsToClean,
        boolean isCleanLivingBroker) throws RemotingException, InterruptedException, MQBrokerException {
        this.defaultMQAdminExtImpl.cleanControllerBrokerData(controllerAddr, clusterName, brokerName, brokerControllerIdsToClean, isCleanLivingBroker);
    }

    @Override
    public void updateColdDataFlowCtrGroupConfig(String brokerAddr, Properties properties)
        throws RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException,
        UnsupportedEncodingException, InterruptedException, MQBrokerException {
        defaultMQAdminExtImpl.updateColdDataFlowCtrGroupConfig(brokerAddr, properties);
    }

    @Override
    public void removeColdDataFlowCtrGroupConfig(String brokerAddr, String consumerGroup)
        throws RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException,
        UnsupportedEncodingException, InterruptedException, MQBrokerException {
        defaultMQAdminExtImpl.removeColdDataFlowCtrGroupConfig(brokerAddr, consumerGroup);
    }

    @Override
    public String getColdDataFlowCtrInfo(String brokerAddr)
        throws RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException,
        UnsupportedEncodingException, InterruptedException, MQBrokerException {
        return defaultMQAdminExtImpl.getColdDataFlowCtrInfo(brokerAddr);
    }

    @Override
    public String setCommitLogReadAheadMode(String brokerAddr, String mode)
        throws RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException,
        InterruptedException, MQBrokerException {
        return defaultMQAdminExtImpl.setCommitLogReadAheadMode(brokerAddr, mode);
    }

    @Override
    public void createUser(String brokerAddr,
        UserInfo userInfo) throws RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, MQBrokerException, InterruptedException {
        defaultMQAdminExtImpl.createUser(brokerAddr, userInfo);
    }

    @Override
    public void createUser(String brokerAddr, String username, String password,
        String userType) throws RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, MQBrokerException, InterruptedException {
        defaultMQAdminExtImpl.createUser(brokerAddr, username, password, userType);
    }

    @Override
    public void updateUser(String brokerAddr, String username,
        String password, String userType,
        String userStatus) throws RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, MQBrokerException, InterruptedException {
        defaultMQAdminExtImpl.updateUser(brokerAddr, username, password, userType, userStatus);
    }

    @Override
    public void updateUser(String brokerAddr,
        UserInfo userInfo) throws RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, MQBrokerException, InterruptedException {
        defaultMQAdminExtImpl.updateUser(brokerAddr, userInfo);
    }

    @Override
    public void deleteUser(String brokerAddr,
        String username) throws RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, MQBrokerException, InterruptedException {
        defaultMQAdminExtImpl.deleteUser(brokerAddr, username);
    }

    @Override
    public UserInfo getUser(String brokerAddr,
        String username) throws RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, MQBrokerException, InterruptedException {
        return defaultMQAdminExtImpl.getUser(brokerAddr, username);
    }

    @Override
    public List<UserInfo> listUser(String brokerAddr,
        String filter) throws RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, MQBrokerException, InterruptedException {
        return defaultMQAdminExtImpl.listUser(brokerAddr, filter);
    }

    @Override
    public void createAcl(String brokerAddr, String subject, List<String> resources, List<String> actions,
        List<String> sourceIps,
        String decision) throws RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, MQBrokerException, InterruptedException {
        defaultMQAdminExtImpl.createAcl(brokerAddr, subject, resources, actions, sourceIps, decision);
    }

    @Override
    public void createAcl(String brokerAddr,
        AclInfo aclInfo) throws RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, MQBrokerException, InterruptedException {
        defaultMQAdminExtImpl.createAcl(brokerAddr, aclInfo);
    }

    @Override
    public void updateAcl(String brokerAddr, String subject, List<String> resources, List<String> actions,
        List<String> sourceIps,
        String decision) throws RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, MQBrokerException, InterruptedException {
        defaultMQAdminExtImpl.updateAcl(brokerAddr, subject, resources, actions, sourceIps, decision);
    }

    @Override
    public void updateAcl(String brokerAddr,
        AclInfo aclInfo) throws RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, MQBrokerException, InterruptedException {
        defaultMQAdminExtImpl.updateAcl(brokerAddr, aclInfo);
    }

    @Override
    public void deleteAcl(String brokerAddr, String subject,
        String resource) throws RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, MQBrokerException, InterruptedException {
        defaultMQAdminExtImpl.deleteAcl(brokerAddr, subject, resource);
    }

    @Override
    public AclInfo getAcl(String brokerAddr,
        String subject) throws RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, MQBrokerException, InterruptedException {
        return defaultMQAdminExtImpl.getAcl(brokerAddr, subject);
    }

    @Override
    public List<AclInfo> listAcl(String brokerAddr, String subjectFilter,
        String resourceFilter) throws RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, MQBrokerException, InterruptedException {
        return defaultMQAdminExtImpl.listAcl(brokerAddr, subjectFilter, resourceFilter);
    }

    @Override
    public void exportPopRecords(String brokerAddr, long timeout) throws RemotingConnectException,
        RemotingSendRequestException, RemotingTimeoutException, MQBrokerException, InterruptedException {
        defaultMQAdminExtImpl.exportPopRecords(brokerAddr, timeout);
    }
}
