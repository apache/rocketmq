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

import com.alibaba.rocketmq.client.MQAdmin;
import com.alibaba.rocketmq.client.exception.MQBrokerException;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.common.TopicConfig;
import com.alibaba.rocketmq.common.admin.ConsumerProgress;
import com.alibaba.rocketmq.common.admin.TopicOffsetTable;
import com.alibaba.rocketmq.common.subscription.SubscriptionGroupConfig;
import com.alibaba.rocketmq.remoting.exception.RemotingException;


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
     * 向指定Broker集群创建或者更新Topic配置
     * 
     * @param cluster
     * @param config
     */
    public void createAndUpdateTopicConfigByCluster(final String cluster, final TopicConfig config);


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
    public void createAndUpdateTopicConfigByAddr(final String addr, final TopicConfig config)
            throws RemotingException, MQBrokerException, InterruptedException, MQClientException;


    /**
     * 向指定Broker集群创建或者更新订阅组配置
     * 
     * @param cluster
     * @param config
     */
    public void createAndUpdateSubscriptionGroupConfigByCluster(final String cluster,
            final SubscriptionGroupConfig config);


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
    public void createAndUpdateSubscriptionGroupConfigByAddr(final String addr,
            final SubscriptionGroupConfig config) throws RemotingException, MQBrokerException,
            InterruptedException, MQClientException;


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
    public TopicOffsetTable examineTopicOffset(final String topic);


    /**
     * 查询消费进度
     * 
     * @param consumerGroup
     * @param topic
     * @return
     */
    public ConsumerProgress examineConsumerProgress(final String consumerGroup, final String topic);


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
