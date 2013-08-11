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

import org.slf4j.Logger;

import com.alibaba.rocketmq.client.QueryResult;
import com.alibaba.rocketmq.client.admin.MQAdminExtInner;
import com.alibaba.rocketmq.client.exception.MQBrokerException;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.client.impl.MQClientManager;
import com.alibaba.rocketmq.client.impl.factory.MQClientFactory;
import com.alibaba.rocketmq.client.log.ClientLogger;
import com.alibaba.rocketmq.common.ServiceState;
import com.alibaba.rocketmq.common.TopicConfig;
import com.alibaba.rocketmq.common.admin.ConsumeStats;
import com.alibaba.rocketmq.common.admin.TopicStatsTable;
import com.alibaba.rocketmq.common.help.FAQUrl;
import com.alibaba.rocketmq.common.message.MessageExt;
import com.alibaba.rocketmq.common.message.MessageQueue;
import com.alibaba.rocketmq.common.protocol.body.ClusterInfo;
import com.alibaba.rocketmq.common.protocol.body.ConsumeByWho;
import com.alibaba.rocketmq.common.protocol.body.ConsumerConnection;
import com.alibaba.rocketmq.common.protocol.body.ProducerConnection;
import com.alibaba.rocketmq.common.protocol.route.BrokerData;
import com.alibaba.rocketmq.common.protocol.route.TopicRouteData;
import com.alibaba.rocketmq.common.subscription.SubscriptionGroupConfig;
import com.alibaba.rocketmq.remoting.exception.RemotingCommandException;
import com.alibaba.rocketmq.remoting.exception.RemotingConnectException;
import com.alibaba.rocketmq.remoting.exception.RemotingException;
import com.alibaba.rocketmq.remoting.exception.RemotingSendRequestException;
import com.alibaba.rocketmq.remoting.exception.RemotingTimeoutException;


/**
 * 所有运维接口都在这里实现
 * 
 * @author shijia.wxr<vintage.wang@gmail.com>
 * @since 2013-7-21
 */
public class DefaultMQAdminExtImpl implements MQAdminExt, MQAdminExtInner {
    private final Logger log = ClientLogger.getLog();
    private final DefaultMQAdminExt defaultMQAdminExt;
    private ServiceState serviceState = ServiceState.CREATE_JUST;
    private MQClientFactory mQClientFactory;


    public DefaultMQAdminExtImpl(DefaultMQAdminExt defaultMQAdminExt) {
        this.defaultMQAdminExt = defaultMQAdminExt;
    }


    @Override
    public void start() throws MQClientException {
        switch (this.serviceState) {
        case CREATE_JUST:
            this.serviceState = ServiceState.RUNNING;

            this.mQClientFactory =
                    MQClientManager.getInstance().getAndCreateMQClientFactory(this.defaultMQAdminExt);

            boolean registerOK =
                    mQClientFactory.registerAdminExt(this.defaultMQAdminExt.getAdminExtGroup(), this);
            if (!registerOK) {
                this.serviceState = ServiceState.CREATE_JUST;
                throw new MQClientException("The adminExt group[" + this.defaultMQAdminExt.getAdminExtGroup()
                        + "] has created already, specifed another name please."//
                        + FAQUrl.suggestTodo(FAQUrl.GROUP_NAME_DUPLICATE_URL), null);
            }

            mQClientFactory.start();

            log.info("the adminExt [{}] start OK", this.defaultMQAdminExt.getAdminExtGroup());
            break;
        case RUNNING:
            break;
        case SHUTDOWN_ALREADY:
            break;
        default:
            break;
        }
    }


    @Override
    public void shutdown() {
        switch (this.serviceState) {
        case CREATE_JUST:
            break;
        case RUNNING:
            this.mQClientFactory.unregisterAdminExt(this.defaultMQAdminExt.getAdminExtGroup());
            this.mQClientFactory.shutdown();

            log.info("the adminExt [{}] shutdown OK", this.defaultMQAdminExt.getAdminExtGroup());
            this.serviceState = ServiceState.SHUTDOWN_ALREADY;
            break;
        case SHUTDOWN_ALREADY:
            break;
        default:
            break;
        }
    }


    @Override
    public void createAndUpdateTopicConfig(String addr, TopicConfig config) throws RemotingException,
            MQBrokerException, InterruptedException, MQClientException {
        this.mQClientFactory.getMQClientAPIImpl().createTopic(addr,
            this.defaultMQAdminExt.getCreateTopicKey(), config, 3000);
    }


    @Override
    public void createAndUpdateSubscriptionGroupConfig(String addr, SubscriptionGroupConfig config)
            throws RemotingException, MQBrokerException, InterruptedException, MQClientException {
        this.mQClientFactory.getMQClientAPIImpl().createSubscriptionGroup(addr, config, 3000);
    }


    @Override
    public SubscriptionGroupConfig examineSubscriptionGroupConfig(String addr, String group) {
        // TODO Auto-generated method stub
        return null;
    }


    @Override
    public TopicConfig examineTopicConfig(String addr, String topic) {
        // TODO Auto-generated method stub
        return null;
    }


    @Override
    public TopicStatsTable examineTopicStats(String topic) throws RemotingException, MQClientException,
            InterruptedException, MQBrokerException {
        TopicRouteData topicRouteData = this.examineTopicRouteInfo(topic);
        TopicStatsTable topicStatsTable = new TopicStatsTable();

        for (BrokerData bd : topicRouteData.getBrokerDatas()) {
            String addr = bd.selectBrokerAddr();
            TopicStatsTable tst =
                    this.mQClientFactory.getMQClientAPIImpl().getTopicStatsInfo(addr, topic, 3000);
            topicStatsTable.getOffsetTable().putAll(tst.getOffsetTable());
        }

        if (topicStatsTable.getOffsetTable().isEmpty()) {
            throw new MQClientException("Not found the topic stats info", null);
        }

        return topicStatsTable;
    }


    @Override
    public ConsumeStats examineConsumerProgress(String consumerGroup, String topic) {
        // TODO Auto-generated method stub
        return null;
    }


    @Override
    public ClusterInfo examineBrokerClusterInfo() throws InterruptedException, MQBrokerException,
            RemotingTimeoutException, RemotingSendRequestException, RemotingConnectException {
        return this.mQClientFactory.getMQClientAPIImpl().getBrokerClusterInfo(3000);
    }


    @Override
    public TopicRouteData examineTopicRouteInfo(String topic) throws RemotingException, MQClientException,
            InterruptedException {
        return this.mQClientFactory.getMQClientAPIImpl().getTopicRouteInfoFromNameServer(topic, 3000);
    }


    @Override
    public void putKVConfig(String namespace, String key, String value) {
        // TODO Auto-generated method stub

    }


    @Override
    public String getKVConfig(String namespace, String key) {
        // TODO Auto-generated method stub
        return null;
    }


    @Override
    public void createTopic(String key, String newTopic, int queueNum) throws MQClientException {
        this.mQClientFactory.getMQAdminImpl().createTopic(key, newTopic, queueNum);
    }


    @Override
    public long searchOffset(MessageQueue mq, long timestamp) throws MQClientException {
        return this.mQClientFactory.getMQAdminImpl().searchOffset(mq, timestamp);
    }


    @Override
    public long maxOffset(MessageQueue mq) throws MQClientException {
        return this.mQClientFactory.getMQAdminImpl().maxOffset(mq);
    }


    @Override
    public long minOffset(MessageQueue mq) throws MQClientException {
        return this.mQClientFactory.getMQAdminImpl().minOffset(mq);
    }


    @Override
    public long earliestMsgStoreTime(MessageQueue mq) throws MQClientException {
        return this.mQClientFactory.getMQAdminImpl().earliestMsgStoreTime(mq);
    }


    @Override
    public MessageExt viewMessage(String msgId) throws RemotingException, MQBrokerException,
            InterruptedException, MQClientException {
        return this.mQClientFactory.getMQAdminImpl().viewMessage(msgId);
    }


    @Override
    public QueryResult queryMessage(String topic, String key, int maxNum, long begin, long end)
            throws MQClientException, InterruptedException {
        return this.mQClientFactory.getMQAdminImpl().queryMessage(topic, key, maxNum, begin, end);
    }


    @Override
    public ConsumerConnection examineConsumerConnectionInfo(String consumerGroup, final String topic)
            throws InterruptedException, MQBrokerException, RemotingException, MQClientException {
        TopicRouteData topicRouteData = this.examineTopicRouteInfo(topic);
        ConsumerConnection result = new ConsumerConnection();

        for (BrokerData bd : topicRouteData.getBrokerDatas()) {
            String addr = bd.selectBrokerAddr();
            return this.mQClientFactory.getMQClientAPIImpl().getConsumerConnectionList(addr, consumerGroup,
                3000);
        }

        if (result.getConnectionSet().isEmpty()) {
            throw new MQClientException("Not found the consumer group connection", null);
        }

        return result;
    }


    @Override
    public ProducerConnection examineProducerConnectionInfo(String producerGroup, final String topic)
            throws RemotingException, MQClientException, InterruptedException, MQBrokerException {
        TopicRouteData topicRouteData = this.examineTopicRouteInfo(topic);
        ProducerConnection result = new ProducerConnection();

        for (BrokerData bd : topicRouteData.getBrokerDatas()) {
            String addr = bd.selectBrokerAddr();
            return this.mQClientFactory.getMQClientAPIImpl().getProducerConnectionList(addr, producerGroup,
                300);
        }

        if (result.getConnectionSet().isEmpty()) {
            throw new MQClientException("Not found the consumer group connection", null);
        }

        return result;
    }


    @Override
    public int wipeWritePermOfBroker(final String namesrvAddr, String brokerName)
            throws RemotingCommandException, RemotingConnectException, RemotingSendRequestException,
            RemotingTimeoutException, InterruptedException, MQClientException {
        return this.mQClientFactory.getMQClientAPIImpl().wipeWritePermOfBroker(namesrvAddr, brokerName, 3000);
    }


    @Override
    public List<String> getNameServerAddressList() {
        return this.mQClientFactory.getMQClientAPIImpl().getNameServerAddressList();
    }


    @Override
    public ConsumeByWho whoConsumeTheMessage(String msgId) {
        // TODO Auto-generated method stub
        return null;
    }
}
