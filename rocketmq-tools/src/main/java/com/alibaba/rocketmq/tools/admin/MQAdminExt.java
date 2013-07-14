package com.alibaba.rocketmq.tools.admin;

import com.alibaba.rocketmq.client.MQAdmin;
import com.alibaba.rocketmq.common.admin.ConsumerProgress;
import com.alibaba.rocketmq.common.admin.TopicOffsetTable;
import com.alibaba.rocketmq.common.subscription.SubscriptionGroupConfig;


/**
 * MQ管理类接口，涉及所有与MQ管理相关的对外接口<br>
 * 包括Topic创建、订阅组创建、配置修改等
 * 
 * @author shijia.wxr<vintage.wang@gmail.com>
 * @since 2013-7-14
 */
public interface MQAdminExt extends MQAdmin {
    public void start();


    public void shutdown();


    /**
     * 向指定Broker集群创建或者更新订阅组配置
     * 
     * @param cluster
     * @param config
     */
    public void createAndUpdateSubscriptionGroupConfig(final String cluster,
            final SubscriptionGroupConfig config);


    /**
     * 查询指定集群的订阅组配置
     * 
     * @param cluster
     * @param group
     * @return
     */
    public SubscriptionGroupConfig examineSubscriptionGroupConfig(final String cluster, final String group);


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
