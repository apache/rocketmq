/**
 * Copyright (C) 2010-2013 Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.rocketmq.tools.admin;

import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import com.alibaba.rocketmq.client.MQAdmin;
import com.alibaba.rocketmq.client.exception.MQBrokerException;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.common.TopicConfig;
import com.alibaba.rocketmq.common.admin.ConsumeStats;
import com.alibaba.rocketmq.common.admin.RollbackStats;
import com.alibaba.rocketmq.common.admin.TopicStatsTable;
import com.alibaba.rocketmq.common.message.MessageExt;
import com.alibaba.rocketmq.common.message.MessageQueue;
import com.alibaba.rocketmq.common.protocol.body.*;
import com.alibaba.rocketmq.common.protocol.route.TopicRouteData;
import com.alibaba.rocketmq.common.subscription.SubscriptionGroupConfig;
import com.alibaba.rocketmq.remoting.exception.*;
import com.alibaba.rocketmq.tools.admin.api.MessageTrack;


/**
 * MQ管理类接口，涉及所有与MQ管理相关的对外接口<br>
 * 包括Topic创建、订阅组创建、配置修改等
 * 
 * @since 2013-7-14
 */
public interface MQAdminExt extends MQAdmin {
    public void start() throws MQClientException;


    public void shutdown();


    /**
     * 更新Broker配置
     * 
     * @param brokerAddr
     * @param properties
     * @throws MQBrokerException
     * @throws InterruptedException
     * @throws UnsupportedEncodingException
     * @throws RemotingTimeoutException
     * @throws RemotingSendRequestException
     * @throws RemotingConnectException
     */
    public void updateBrokerConfig(final String brokerAddr, final Properties properties)
            throws RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException,
            UnsupportedEncodingException, InterruptedException, MQBrokerException;


    /**
     * 向指定Broker创建或者更新Topic配置
     * 
     * @param addr
     * @param config
     * @throws MQClientException
     * @throws InterruptedException
     * @throws MQBrokerException
     * @throws RemotingException
     */
    public void createAndUpdateTopicConfig(final String addr, final TopicConfig config)
            throws RemotingException, MQBrokerException, InterruptedException, MQClientException;


    /**
     * 向指定Broker创建或者更新订阅组配置
     * 
     * @param addr
     * @param config
     * @throws MQClientException
     * @throws InterruptedException
     * @throws MQBrokerException
     * @throws RemotingException
     */
    public void createAndUpdateSubscriptionGroupConfig(final String addr, final SubscriptionGroupConfig config)
            throws RemotingException, MQBrokerException, InterruptedException, MQClientException;


    /**
     * 查询指定Broker的订阅组配置
     * 
     * @param addr
     * @param group
     * @return
     */
    public SubscriptionGroupConfig examineSubscriptionGroupConfig(final String addr, final String group);


    /**
     * 查询指定Broker的Topic配置
     * 
     * @param addr
     * @param topic
     * @return
     */
    public TopicConfig examineTopicConfig(final String addr, final String topic);


    /**
     * 查询Topic Offset信息
     * 
     * @param topic
     * @return
     */
    public TopicStatsTable examineTopicStats(final String topic) throws RemotingException, MQClientException,
            InterruptedException, MQBrokerException;


    /**
     * 从Name Server获取所有Topic列表
     * 
     * @return
     * @throws InterruptedException
     * @throws MQClientException
     * @throws RemotingException
     */
    public TopicList fetchAllTopicList() throws RemotingException, MQClientException, InterruptedException;


    /**
     * 获取Broker运行时数据
     * 
     * @return
     * @throws MQBrokerException
     * @throws InterruptedException
     * @throws RemotingTimeoutException
     * @throws RemotingSendRequestException
     * @throws RemotingConnectException
     */
    public KVTable fetchBrokerRuntimeStats(final String brokerAddr) throws RemotingConnectException,
            RemotingSendRequestException, RemotingTimeoutException, InterruptedException, MQBrokerException;


    /**
     * 查询消费进度
     * 
     * @param consumerGroup
     * @return
     * @throws InterruptedException
     * @throws MQClientException
     * @throws RemotingException
     * @throws MQBrokerException
     */
    public ConsumeStats examineConsumeStats(final String consumerGroup) throws RemotingException,
            MQClientException, InterruptedException, MQBrokerException;


    public ConsumeStats examineConsumeStats(final String consumerGroup, final String topic)
            throws RemotingException, MQClientException, InterruptedException, MQBrokerException;


    /**
     * 查看集群信息
     * 
     * @return
     */
    public ClusterInfo examineBrokerClusterInfo() throws InterruptedException, MQBrokerException,
            RemotingTimeoutException, RemotingSendRequestException, RemotingConnectException;


    /**
     * 查看Topic路由信息
     * 
     * @param topic
     * @return
     */
    public TopicRouteData examineTopicRouteInfo(final String topic) throws RemotingException,
            MQClientException, InterruptedException;


    /**
     * 查看Consumer网络连接、订阅关系
     * 
     * @param consumerGroup
     * @return
     * @throws MQBrokerException
     * @throws InterruptedException
     * @throws RemotingTimeoutException
     * @throws RemotingSendRequestException
     * @throws RemotingConnectException
     * @throws MQClientException
     * @throws RemotingException
     */
    public ConsumerConnection examineConsumerConnectionInfo(final String consumerGroup)
            throws RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException,
            InterruptedException, MQBrokerException, RemotingException, MQClientException;


    /**
     * 查看Producer网络连接
     * 
     * @param producerGroup
     * @param topic
     * @return
     * @throws InterruptedException
     * @throws MQClientException
     * @throws RemotingException
     * @throws MQBrokerException
     */
    public ProducerConnection examineProducerConnectionInfo(final String producerGroup, final String topic)
            throws RemotingException, MQClientException, InterruptedException, MQBrokerException;


    /**
     * 获取Name Server地址列表
     * 
     * @return
     */
    public List<String> getNameServerAddressList();


    /**
     * 清除某个Broker的写权限，针对所有Name Server
     * 
     * @param brokerName
     * @return 返回清除了多少个topic
     * @throws MQClientException
     * @throws InterruptedException
     * @throws RemotingTimeoutException
     * @throws RemotingSendRequestException
     * @throws RemotingConnectException
     * @throws RemotingCommandException
     */
    public int wipeWritePermOfBroker(final String namesrvAddr, String brokerName)
            throws RemotingCommandException, RemotingConnectException, RemotingSendRequestException,
            RemotingTimeoutException, InterruptedException, MQClientException;


    /**
     * 向Name Server增加一个配置项
     * 
     * @param namespace
     * @param key
     * @param value
     */
    public void putKVConfig(final String namespace, final String key, final String value);


    /**
     * 从Name Server获取一个配置项
     * 
     * @param namespace
     * @param key
     * @return
     */
    public String getKVConfig(final String namespace, final String key) throws RemotingException,
            MQClientException, InterruptedException;


    /**
     * 获取指定Namespace下的所有kv
     * 
     * @param namespace
     * @return
     * @throws InterruptedException
     * @throws MQClientException
     * @throws RemotingException
     */
    public KVTable getKVListByNamespace(final String namespace) throws RemotingException, MQClientException,
            InterruptedException;


    /**
     * 删除 broker 上的 topic 信息
     * 
     * @param addrs
     * @param topic
     * @throws RemotingException
     * @throws MQBrokerException
     * @throws InterruptedException
     * @throws MQClientException
     */
    public void deleteTopicInBroker(final Set<String> addrs, final String topic) throws RemotingException,
            MQBrokerException, InterruptedException, MQClientException;


    /**
     * 删除 broker 上的 topic 信息
     * 
     * @param addrs
     * @param topic
     * @throws RemotingException
     * @throws MQBrokerException
     * @throws InterruptedException
     * @throws MQClientException
     */
    public void deleteTopicInNameServer(final Set<String> addrs, final String topic)
            throws RemotingException, MQBrokerException, InterruptedException, MQClientException;


    /**
     * 删除 broker 上的 subscription group 信息
     * 
     * @param addr
     * @param groupName
     * @throws RemotingException
     * @throws MQBrokerException
     * @throws InterruptedException
     * @throws MQClientException
     */
    public void deleteSubscriptionGroup(final String addr, String groupName) throws RemotingException,
            MQBrokerException, InterruptedException, MQClientException;


    /**
     * 在 namespace 上添加或者更新 KV 配置
     * 
     * @param namespace
     * @param key
     * @param value
     * @throws RemotingException
     * @throws MQBrokerException
     * @throws InterruptedException
     * @throws MQClientException
     */
    public void createAndUpdateKvConfig(String namespace, String key, String value) throws RemotingException,
            MQBrokerException, InterruptedException, MQClientException;


    /**
     * 删除 namespace 上的 KV 配置
     * 
     * @param namespace
     * @param key
     * @throws RemotingException
     * @throws MQBrokerException
     * @throws InterruptedException
     * @throws MQClientException
     */
    public void deleteKvConfig(String namespace, String key) throws RemotingException, MQBrokerException,
            InterruptedException, MQClientException;


    /**
     * 通过 server ip 获取 project 信息
     * 
     * @param ip
     * @throws RemotingException
     * @throws MQBrokerException
     * @throws InterruptedException
     * @throws MQClientException
     * @return
     */
    public String getProjectGroupByIp(String ip) throws RemotingException, MQBrokerException,
            InterruptedException, MQClientException;


    /**
     * 通过 project 获取所有的 server ip 信息
     * 
     * @param projectGroup
     * @throws RemotingException
     * @throws MQBrokerException
     * @throws InterruptedException
     * @throws MQClientException
     * @return
     */
    public String getIpsByProjectGroup(String projectGroup) throws RemotingException, MQBrokerException,
            InterruptedException, MQClientException;


    /**
     * 删除 project group 对应的所有 server ip
     * 
     * @param key
     * @throws RemotingException
     * @throws MQBrokerException
     * @throws InterruptedException
     * @throws MQClientException
     */
    public void deleteIpsByProjectGroup(String key) throws RemotingException, MQBrokerException,
            InterruptedException, MQClientException;


    /**
     * 按照时间回溯消费进度(客户端需要重启)
     * 
     * @param consumerGroup
     * @param topic
     * @param timestamp
     * @param force
     * @throws RemotingException
     * @throws MQBrokerException
     * @throws InterruptedException
     * @throws MQClientException
     * @return
     */
    public List<RollbackStats> resetOffsetByTimestampOld(String consumerGroup, String topic, long timestamp,
            boolean force) throws RemotingException, MQBrokerException, InterruptedException,
            MQClientException;


    /**
     * 按照时间回溯消费进度(客户端不需要重启)
     * 
     * @param topic
     * @param group
     * @param timestamp
     * @param isForce
     * @throws RemotingException
     * @throws MQBrokerException
     * @throws InterruptedException
     * @throws MQClientException
     * @return
     */
    public Map<MessageQueue, Long> resetOffsetByTimestamp(String topic, String group, long timestamp,
            boolean isForce) throws RemotingException, MQBrokerException, InterruptedException,
            MQClientException;


    /**
     * 重置消费进度，无论Consumer是否在线，都可以执行。不保证最终结果是否成功，需要调用方通过消费进度查询来再次确认
     * 
     * @param consumerGroup
     * @param topic
     * @param timestamp
     * @throws InterruptedException
     * @throws MQBrokerException
     * @throws RemotingException
     * @throws MQClientException
     */
    public void resetOffsetNew(String consumerGroup, String topic, long timestamp) throws RemotingException,
            MQBrokerException, InterruptedException, MQClientException;


    /**
     * 通过客户端查看消费者的消费情况
     * 
     * @param topic
     * @param group
     * @param clientAddr
     * @return
     * @throws RemotingException
     * @throws MQBrokerException
     * @throws InterruptedException
     * @throws MQClientException
     */
    public Map<String, Map<MessageQueue, Long>> getConsumeStatus(String topic, String group, String clientAddr)
            throws RemotingException, MQBrokerException, InterruptedException, MQClientException;


    /**
     * 创建或更新顺序消息的分区配置
     * 
     * @param key
     * @param value
     * @param isCluster
     * @throws RemotingException
     * @throws MQBrokerException
     * @throws InterruptedException
     * @throws MQClientException
     */
    public void createOrUpdateOrderConf(String key, String value, boolean isCluster)
            throws RemotingException, MQBrokerException, InterruptedException, MQClientException;


    /**
     * 根据Topic查询被哪些订阅组消费
     * 
     * @param topic
     * @return
     * @throws MQBrokerException
     * @throws InterruptedException
     * @throws RemotingTimeoutException
     * @throws RemotingSendRequestException
     * @throws RemotingConnectException
     * @throws MQClientException
     * @throws RemotingException
     */
    public GroupList queryTopicConsumeByWho(final String topic) throws RemotingConnectException,
            RemotingSendRequestException, RemotingTimeoutException, InterruptedException, MQBrokerException,
            RemotingException, MQClientException;


    /**
     * 根据 topic 和 group 获取消息的时间跨度
     * 
     * @param topic
     * @param group
     * @return
     * @throws RemotingConnectException
     * @throws RemotingSendRequestException
     * @throws RemotingTimeoutException
     * @throws InterruptedException
     * @throws MQBrokerException
     * @throws RemotingException
     * @throws MQClientException
     */
    public Set<QueueTimeSpan> queryConsumeTimeSpan(final String topic, final String group)
            throws InterruptedException, MQBrokerException, RemotingException, MQClientException;


    /**
     * 触发清理失效的消费队列
     * 
     * @param cluster
     *            null则表示所有集群
     * @return 清理是否成功
     * @throws RemotingConnectException
     * @throws RemotingSendRequestException
     * @throws RemotingTimeoutException
     * @throws MQClientException
     * @throws InterruptedException
     */
    public boolean cleanExpiredConsumerQueue(String cluster) throws RemotingConnectException,
            RemotingSendRequestException, RemotingTimeoutException, MQClientException, InterruptedException;


    /**
     * 触发指定的broker清理失效的消费队列
     * 
     * @param addr
     * @return 清理是否成功
     * @throws RemotingConnectException
     * @throws RemotingSendRequestException
     * @throws RemotingTimeoutException
     * @throws MQClientException
     * @throws InterruptedException
     */
    public boolean cleanExpiredConsumerQueueByAddr(String addr) throws RemotingConnectException,
            RemotingSendRequestException, RemotingTimeoutException, MQClientException, InterruptedException;


    /**
     * 查询Consumer内存数据结构
     * 
     * @param consumerGroup
     * @param clientId
     * @return
     * @throws InterruptedException
     * @throws MQClientException
     * @throws RemotingException
     */
    public ConsumerRunningInfo getConsumerRunningInfo(final String consumerGroup, final String clientId,
            final boolean jstack) throws RemotingException, MQClientException, InterruptedException;


    /**
     * 向指定Consumer发送某条消息
     * 
     * @param consumerGroup
     * @param clientId
     * @param msgId
     * @return
     * @throws InterruptedException
     * @throws MQClientException
     * @throws RemotingException
     * @throws MQBrokerException
     */
    public ConsumeMessageDirectlyResult consumeMessageDirectly(String consumerGroup, //
            String clientId, //
            String msgId) throws RemotingException, MQClientException, InterruptedException,
            MQBrokerException;


    /**
     * 查询消息被谁消费了
     * 
     * @param msg
     * @return
     * @throws RemotingException
     * @throws MQClientException
     * @throws InterruptedException
     * @throws MQBrokerException
     */
    public List<MessageTrack> messageTrackDetail(MessageExt msg) throws RemotingException, MQClientException,
            InterruptedException, MQBrokerException;


    /**
     * 克隆某一个组的消费进度到新的组
     * 
     * @param srcGroup
     * @param destGroup
     * @param topic
     * @param isOffline
     * @throws RemotingException
     * @throws MQClientException
     * @throws InterruptedException
     * @throws MQBrokerException
     */
    public void cloneGroupOffset(String srcGroup, String destGroup, String topic, boolean isOffline)
            throws RemotingException, MQClientException, InterruptedException, MQBrokerException;


    /**
     * 服务器统计数据输出
     * 
     * @param statsName
     * @param statsKey
     * @return
     * @throws InterruptedException
     * @throws MQClientException
     * @throws RemotingTimeoutException
     * @throws RemotingSendRequestException
     * @throws RemotingConnectException
     */
    public BrokerStatsData ViewBrokerStatsData(final String brokerAddr, final String statsName,
            final String statsKey) throws RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, MQClientException, InterruptedException;
}
