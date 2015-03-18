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
package com.alibaba.rocketmq.client.impl;

import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;

import com.alibaba.rocketmq.client.QueryResult;
import com.alibaba.rocketmq.client.exception.MQBrokerException;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.client.impl.factory.MQClientInstance;
import com.alibaba.rocketmq.client.impl.producer.TopicPublishInfo;
import com.alibaba.rocketmq.client.log.ClientLogger;
import com.alibaba.rocketmq.common.MixAll;
import com.alibaba.rocketmq.common.TopicConfig;
import com.alibaba.rocketmq.common.help.FAQUrl;
import com.alibaba.rocketmq.common.message.MessageConst;
import com.alibaba.rocketmq.common.message.MessageDecoder;
import com.alibaba.rocketmq.common.message.MessageExt;
import com.alibaba.rocketmq.common.message.MessageId;
import com.alibaba.rocketmq.common.message.MessageQueue;
import com.alibaba.rocketmq.common.protocol.ResponseCode;
import com.alibaba.rocketmq.common.protocol.header.QueryMessageRequestHeader;
import com.alibaba.rocketmq.common.protocol.header.QueryMessageResponseHeader;
import com.alibaba.rocketmq.common.protocol.route.BrokerData;
import com.alibaba.rocketmq.common.protocol.route.TopicRouteData;
import com.alibaba.rocketmq.remoting.InvokeCallback;
import com.alibaba.rocketmq.remoting.common.RemotingUtil;
import com.alibaba.rocketmq.remoting.exception.RemotingCommandException;
import com.alibaba.rocketmq.remoting.exception.RemotingException;
import com.alibaba.rocketmq.remoting.netty.ResponseFuture;
import com.alibaba.rocketmq.remoting.protocol.RemotingCommand;


/**
 * @author shijia.wxr<vintage.wang@gmail.com>
 * @since 2013-7-24
 */
public class MQAdminImpl {
    private final Logger log = ClientLogger.getLog();
    private final MQClientInstance mQClientFactory;


    public MQAdminImpl(MQClientInstance mQClientFactory) {
        this.mQClientFactory = mQClientFactory;
    }


    public void createTopic(String key, String newTopic, int queueNum) throws MQClientException {
        createTopic(key, newTopic, queueNum, 0);
    }


    public void createTopic(String key, String newTopic, int queueNum, int topicSysFlag)
            throws MQClientException {
        try {
            TopicRouteData topicRouteData =
                    this.mQClientFactory.getMQClientAPIImpl().getTopicRouteInfoFromNameServer(key, 1000 * 3);
            List<BrokerData> brokerDataList = topicRouteData.getBrokerDatas();
            if (brokerDataList != null && !brokerDataList.isEmpty()) {
                Collections.sort(brokerDataList);

                MQClientException exception = null;

                StringBuilder orderTopicString = new StringBuilder();

                for (BrokerData brokerData : brokerDataList) {
                    String addr = brokerData.getBrokerAddrs().get(MixAll.MASTER_ID);
                    if (addr != null) {
                        TopicConfig topicConfig = new TopicConfig(newTopic);
                        topicConfig.setReadQueueNums(queueNum);
                        topicConfig.setWriteQueueNums(queueNum);
                        topicConfig.setTopicSysFlag(topicSysFlag);
                        try {
                            this.mQClientFactory.getMQClientAPIImpl().createTopic(addr, key, topicConfig,
                                1000 * 3);
                        }
                        catch (Exception e) {
                            exception = new MQClientException("create topic to broker exception", e);
                        }

                        orderTopicString.append(brokerData.getBrokerName());
                        orderTopicString.append(":");
                        orderTopicString.append(queueNum);
                        orderTopicString.append(";");
                    }
                }

                if (exception != null) {
                    throw exception;
                }
            }
            else {
                throw new MQClientException("Not found broker, maybe key is wrong", null);
            }
        }
        catch (Exception e) {
            throw new MQClientException("create new topic failed", e);
        }
    }


    public List<MessageQueue> fetchPublishMessageQueues(String topic) throws MQClientException {
        try {
            TopicRouteData topicRouteData =
                    this.mQClientFactory.getMQClientAPIImpl()
                        .getTopicRouteInfoFromNameServer(topic, 1000 * 3);
            if (topicRouteData != null) {
                TopicPublishInfo topicPublishInfo =
                        MQClientInstance.topicRouteData2TopicPublishInfo(topic, topicRouteData);
                if (topicPublishInfo != null && topicPublishInfo.ok()) {
                    return topicPublishInfo.getMessageQueueList();
                }
            }
        }
        catch (Exception e) {
            throw new MQClientException("Can not find Message Queue for this topic, " + topic, e);
        }

        throw new MQClientException("Unknow why, Can not find Message Queue for this topic, " + topic, null);
    }


    public Set<MessageQueue> fetchSubscribeMessageQueues(String topic) throws MQClientException {
        try {
            TopicRouteData topicRouteData =
                    this.mQClientFactory.getMQClientAPIImpl()
                        .getTopicRouteInfoFromNameServer(topic, 1000 * 3);
            if (topicRouteData != null) {
                Set<MessageQueue> mqList =
                        MQClientInstance.topicRouteData2TopicSubscribeInfo(topic, topicRouteData);
                if (!mqList.isEmpty()) {
                    return mqList;
                }
                else {
                    throw new MQClientException("Can not find Message Queue for this topic, " + topic
                            + " Namesrv return empty", null);
                }
            }
        }
        catch (Exception e) {
            throw new MQClientException("Can not find Message Queue for this topic, " + topic
                    + FAQUrl.suggestTodo(FAQUrl.MQLIST_NOT_EXIST), //
                e);
        }

        throw new MQClientException("Unknow why, Can not find Message Queue for this topic, " + topic, null);
    }


    public long searchOffset(MessageQueue mq, long timestamp) throws MQClientException {
        String brokerAddr = this.mQClientFactory.findBrokerAddressInPublish(mq.getBrokerName());
        if (null == brokerAddr) {
            this.mQClientFactory.updateTopicRouteInfoFromNameServer(mq.getTopic());
            brokerAddr = this.mQClientFactory.findBrokerAddressInPublish(mq.getBrokerName());
        }

        if (brokerAddr != null) {
            try {
                return this.mQClientFactory.getMQClientAPIImpl().searchOffset(brokerAddr, mq.getTopic(),
                    mq.getQueueId(), timestamp, 1000 * 3);
            }
            catch (Exception e) {
                throw new MQClientException("Invoke Broker[" + brokerAddr + "] exception", e);
            }
        }

        throw new MQClientException("The broker[" + mq.getBrokerName() + "] not exist", null);
    }


    public long maxOffset(MessageQueue mq) throws MQClientException {
        String brokerAddr = this.mQClientFactory.findBrokerAddressInPublish(mq.getBrokerName());
        if (null == brokerAddr) {
            this.mQClientFactory.updateTopicRouteInfoFromNameServer(mq.getTopic());
            brokerAddr = this.mQClientFactory.findBrokerAddressInPublish(mq.getBrokerName());
        }

        if (brokerAddr != null) {
            try {
                return this.mQClientFactory.getMQClientAPIImpl().getMaxOffset(brokerAddr, mq.getTopic(),
                    mq.getQueueId(), 1000 * 3);
            }
            catch (Exception e) {
                throw new MQClientException("Invoke Broker[" + brokerAddr + "] exception", e);
            }
        }

        throw new MQClientException("The broker[" + mq.getBrokerName() + "] not exist", null);
    }


    public long minOffset(MessageQueue mq) throws MQClientException {
        String brokerAddr = this.mQClientFactory.findBrokerAddressInPublish(mq.getBrokerName());
        if (null == brokerAddr) {
            this.mQClientFactory.updateTopicRouteInfoFromNameServer(mq.getTopic());
            brokerAddr = this.mQClientFactory.findBrokerAddressInPublish(mq.getBrokerName());
        }

        if (brokerAddr != null) {
            try {
                return this.mQClientFactory.getMQClientAPIImpl().getMinOffset(brokerAddr, mq.getTopic(),
                    mq.getQueueId(), 1000 * 3);
            }
            catch (Exception e) {
                throw new MQClientException("Invoke Broker[" + brokerAddr + "] exception", e);
            }
        }

        throw new MQClientException("The broker[" + mq.getBrokerName() + "] not exist", null);
    }


    public long earliestMsgStoreTime(MessageQueue mq) throws MQClientException {
        String brokerAddr = this.mQClientFactory.findBrokerAddressInPublish(mq.getBrokerName());
        if (null == brokerAddr) {
            this.mQClientFactory.updateTopicRouteInfoFromNameServer(mq.getTopic());
            brokerAddr = this.mQClientFactory.findBrokerAddressInPublish(mq.getBrokerName());
        }

        if (brokerAddr != null) {
            try {
                return this.mQClientFactory.getMQClientAPIImpl().getEarliestMsgStoretime(brokerAddr,
                    mq.getTopic(), mq.getQueueId(), 1000 * 3);
            }
            catch (Exception e) {
                throw new MQClientException("Invoke Broker[" + brokerAddr + "] exception", e);
            }
        }

        throw new MQClientException("The broker[" + mq.getBrokerName() + "] not exist", null);
    }


    public MessageExt viewMessage(String msgId) throws RemotingException, MQBrokerException,
            InterruptedException, MQClientException {
        try {
            MessageId messageId = MessageDecoder.decodeMessageId(msgId);
            return this.mQClientFactory.getMQClientAPIImpl().viewMessage(
                RemotingUtil.socketAddress2String(messageId.getAddress()), messageId.getOffset(), 1000 * 3);
        }
        catch (UnknownHostException e) {
            throw new MQClientException("message id illegal", e);
        }
    }


    public QueryResult queryMessage(String topic, String key, int maxNum, long begin, long end)
            throws MQClientException, InterruptedException {
        TopicRouteData topicRouteData = this.mQClientFactory.getAnExistTopicRouteData(topic);
        if (null == topicRouteData) {
            this.mQClientFactory.updateTopicRouteInfoFromNameServer(topic);
            topicRouteData = this.mQClientFactory.getAnExistTopicRouteData(topic);
        }

        if (topicRouteData != null) {
            List<String> brokerAddrs = new LinkedList<String>();
            for (BrokerData brokerData : topicRouteData.getBrokerDatas()) {
                String addr = brokerData.selectBrokerAddr();
                if (addr != null) {
                    brokerAddrs.add(addr);
                }
            }

            if (!brokerAddrs.isEmpty()) {
                final CountDownLatch countDownLatch = new CountDownLatch(brokerAddrs.size());
                final List<QueryResult> queryResultList = new LinkedList<QueryResult>();

                for (String addr : brokerAddrs) {
                    try {
                        QueryMessageRequestHeader requestHeader = new QueryMessageRequestHeader();
                        requestHeader.setTopic(topic);
                        requestHeader.setKey(key);
                        requestHeader.setMaxNum(maxNum);
                        requestHeader.setBeginTimestamp(begin);
                        requestHeader.setEndTimestamp(end);

                        this.mQClientFactory.getMQClientAPIImpl().queryMessage(addr, requestHeader,
                            1000 * 15, new InvokeCallback() {
                                @Override
                                public void operationComplete(ResponseFuture responseFuture) {
                                    try {
                                        RemotingCommand response = responseFuture.getResponseCommand();
                                        if (response != null) {
                                            switch (response.getCode()) {
                                            case ResponseCode.SUCCESS: {
                                                QueryMessageResponseHeader responseHeader = null;
                                                try {
                                                    responseHeader =
                                                            (QueryMessageResponseHeader) response
                                                                .decodeCommandCustomHeader(QueryMessageResponseHeader.class);
                                                }
                                                catch (RemotingCommandException e) {
                                                    log.error("decodeCommandCustomHeader exception", e);
                                                    return;
                                                }

                                                List<MessageExt> wrappers =
                                                        MessageDecoder.decodes(
                                                            ByteBuffer.wrap(response.getBody()), true);

                                                QueryResult qr =
                                                        new QueryResult(responseHeader
                                                            .getIndexLastUpdateTimestamp(), wrappers);
                                                queryResultList.add(qr);
                                                break;
                                            }
                                            default:
                                                log.warn("getResponseCommand failed, {} {}",
                                                    response.getCode(), response.getRemark());
                                                break;
                                            }
                                        }
                                        else {
                                            log.warn("getResponseCommand return null");
                                        }
                                    }
                                    finally {
                                        countDownLatch.countDown();
                                    }
                                }
                            });
                    }
                    catch (Exception e) {
                        log.warn("queryMessage exception", e);
                    }// end of try

                } // end of for

                boolean ok = countDownLatch.await(1000 * 20, TimeUnit.MILLISECONDS);
                if (!ok) {
                    log.warn("queryMessage, maybe some broker failed");
                }

                long indexLastUpdateTimestamp = 0;
                List<MessageExt> messageList = new LinkedList<MessageExt>();
                for (QueryResult qr : queryResultList) {
                    if (qr.getIndexLastUpdateTimestamp() > indexLastUpdateTimestamp) {
                        indexLastUpdateTimestamp = qr.getIndexLastUpdateTimestamp();
                    }

                    for (MessageExt msgExt : qr.getMessageList()) {
                        String keys = msgExt.getKeys();
                        if (keys != null) {
                            boolean matched = false;
                            String[] keyArray = keys.split(MessageConst.KEY_SEPARATOR);
                            if (keyArray != null) {
                                for (String k : keyArray) {
                                    if (key.equals(k)) {
                                        matched = true;
                                        break;
                                    }
                                }
                            }

                            if (matched) {
                                messageList.add(msgExt);
                            }
                            else {
                                log.warn(
                                    "queryMessage, find message key not matched, maybe hash duplicate {}",
                                    msgExt.toString());
                            }
                        }
                    }
                }

                if (!messageList.isEmpty()) {
                    return new QueryResult(indexLastUpdateTimestamp, messageList);
                }
                else {
                    throw new MQClientException("query operation over, but no message.", null);
                }
            }
        }

        throw new MQClientException("The topic[" + topic + "] not matched route info", null);
    }
}
