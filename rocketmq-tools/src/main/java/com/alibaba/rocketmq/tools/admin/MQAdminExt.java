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

import java.util.List;

import com.alibaba.rocketmq.client.MQAdmin;
import com.alibaba.rocketmq.client.exception.MQBrokerException;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.common.TopicConfig;
import com.alibaba.rocketmq.common.admin.ConsumeStats;
import com.alibaba.rocketmq.common.admin.TopicStatsTable;
import com.alibaba.rocketmq.common.protocol.body.ClusterInfo;
import com.alibaba.rocketmq.common.protocol.body.ConsumeByWho;
import com.alibaba.rocketmq.common.protocol.body.ConsumerConnection;
import com.alibaba.rocketmq.common.protocol.body.ProducerConnection;
import com.alibaba.rocketmq.common.protocol.route.TopicRouteData;
import com.alibaba.rocketmq.common.subscription.SubscriptionGroupConfig;
import com.alibaba.rocketmq.remoting.exception.RemotingCommandException;
import com.alibaba.rocketmq.remoting.exception.RemotingConnectException;
import com.alibaba.rocketmq.remoting.exception.RemotingException;
import com.alibaba.rocketmq.remoting.exception.RemotingSendRequestException;
import com.alibaba.rocketmq.remoting.exception.RemotingTimeoutException;


/**
 * MQ管理类接口，涉及所有与MQ管理相关的对外接口<br>
 * 包括Topic创建、订阅组创建、配置修改等
 * 
 * @author shijia.wxr<vintage.wang@gmail.com>
 * @since 2013-7-14
 */
public interface MQAdminExt extends MQAdmin {
    public void start() throws MQClientException;


    public void shutdown();


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
     * @param group
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
     * 查询消费进度
     * 
     * @param consumerGroup
     * @param topic
     * @return
     */
    public ConsumeStats examineConsumerProgress(final String consumerGroup, final String topic);


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
    public ConsumerConnection examineConsumerConnectionInfo(final String consumerGroup, final String topic)
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
     * 查看某个订阅组被谁消费了
     * 
     * @param msgId
     * @return
     */
    public ConsumeByWho whoConsumeTheMessage(final String msgId);


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
    public String getKVConfig(final String namespace, final String key);
}
