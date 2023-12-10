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
package org.apache.rocketmq.client.impl;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.rocketmq.client.QueryResult;
import org.apache.rocketmq.client.Validators;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.client.impl.producer.TopicPublishInfo;
import org.apache.rocketmq.common.BoundaryType;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.help.FAQUrl;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageId;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.utils.NetworkUtil;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.InvokeCallback;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.apache.rocketmq.remoting.netty.ResponseFuture;
import org.apache.rocketmq.remoting.protocol.NamespaceUtil;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.ResponseCode;
import org.apache.rocketmq.remoting.protocol.header.QueryMessageRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.QueryMessageResponseHeader;
import org.apache.rocketmq.remoting.protocol.route.BrokerData;
import org.apache.rocketmq.remoting.protocol.route.TopicRouteData;

public class MQAdminImpl {

    private static final Logger log = LoggerFactory.getLogger(MQAdminImpl.class);
    private final MQClientInstance mqClientFactory;
    private long timeoutMillis = 6000;

    public MQAdminImpl(MQClientInstance mqClientFactory) {
        this.mqClientFactory = mqClientFactory;
    }

    public long getTimeoutMillis() {
        return timeoutMillis;
    }

    public void setTimeoutMillis(long timeoutMillis) {
        this.timeoutMillis = timeoutMillis;
    }

    public void createTopic(String key, String newTopic, int queueNum) throws MQClientException {
        createTopic(key, newTopic, queueNum, 0, null);
    }

    public void createTopic(String key, String newTopic, int queueNum, int topicSysFlag,
        Map<String, String> attributes) throws MQClientException {
        try {
            Validators.checkTopic(newTopic);
            Validators.isSystemTopic(newTopic);
            TopicRouteData topicRouteData = this.mqClientFactory.getMQClientAPIImpl().getTopicRouteInfoFromNameServer(key, timeoutMillis);
            List<BrokerData> brokerDataList = topicRouteData.getBrokerDatas();
            if (brokerDataList != null && !brokerDataList.isEmpty()) {
                Collections.sort(brokerDataList);

                boolean createOKAtLeastOnce = false;
                MQClientException exception = null;

                StringBuilder orderTopicString = new StringBuilder();

                for (BrokerData brokerData : brokerDataList) {
                    String addr = brokerData.getBrokerAddrs().get(MixAll.MASTER_ID);
                    if (addr != null) {
                        TopicConfig topicConfig = new TopicConfig(newTopic);
                        topicConfig.setReadQueueNums(queueNum);
                        topicConfig.setWriteQueueNums(queueNum);
                        topicConfig.setTopicSysFlag(topicSysFlag);
                        topicConfig.setAttributes(attributes);

                        boolean createOK = false;
                        for (int i = 0; i < 5; i++) {
                            try {
                                this.mqClientFactory.getMQClientAPIImpl().createTopic(addr, key, topicConfig, timeoutMillis);
                                createOK = true;
                                createOKAtLeastOnce = true;
                                break;
                            } catch (Exception e) {
                                if (4 == i) {
                                    exception = new MQClientException("create topic to broker exception", e);
                                }
                            }
                        }

                        if (createOK) {
                            orderTopicString.append(brokerData.getBrokerName());
                            orderTopicString.append(":");
                            orderTopicString.append(queueNum);
                            orderTopicString.append(";");
                        }
                    }
                }

                if (exception != null && !createOKAtLeastOnce) {
                    throw exception;
                }
            } else {
                throw new MQClientException("Not found broker, maybe key is wrong", null);
            }
        } catch (Exception e) {
            throw new MQClientException("create new topic failed", e);
        }
    }

    public List<MessageQueue> fetchPublishMessageQueues(String topic) throws MQClientException {
        try {
            TopicRouteData topicRouteData = this.mqClientFactory.getMQClientAPIImpl().getTopicRouteInfoFromNameServer(topic, timeoutMillis);
            if (topicRouteData != null) {
                TopicPublishInfo topicPublishInfo = MQClientInstance.topicRouteData2TopicPublishInfo(topic, topicRouteData);
                if (topicPublishInfo != null && topicPublishInfo.ok()) {
                    return parsePublishMessageQueues(topicPublishInfo.getMessageQueueList());
                }
            }
        } catch (Exception e) {
            throw new MQClientException("Can not find Message Queue for this topic, " + topic, e);
        }

        throw new MQClientException("Unknow why, Can not find Message Queue for this topic, " + topic, null);
    }

    public List<MessageQueue> parsePublishMessageQueues(List<MessageQueue> messageQueueList) {
        List<MessageQueue> resultQueues = new ArrayList<>();
        for (MessageQueue queue : messageQueueList) {
            String userTopic = NamespaceUtil.withoutNamespace(queue.getTopic(), this.mqClientFactory.getClientConfig().getNamespace());
            resultQueues.add(new MessageQueue(userTopic, queue.getBrokerName(), queue.getQueueId()));
        }

        return resultQueues;
    }

    public Set<MessageQueue> fetchSubscribeMessageQueues(String topic) throws MQClientException {
        try {
            TopicRouteData topicRouteData = this.mqClientFactory.getMQClientAPIImpl().getTopicRouteInfoFromNameServer(topic, timeoutMillis);
            if (topicRouteData != null) {
                Set<MessageQueue> mqList = MQClientInstance.topicRouteData2TopicSubscribeInfo(topic, topicRouteData);
                if (!mqList.isEmpty()) {
                    return mqList;
                } else {
                    throw new MQClientException("Can not find Message Queue for this topic, " + topic + " Namesrv return empty", null);
                }
            }
        } catch (Exception e) {
            throw new MQClientException(
                "Can not find Message Queue for this topic, " + topic + FAQUrl.suggestTodo(FAQUrl.MQLIST_NOT_EXIST),
                e);
        }

        throw new MQClientException("Unknow why, Can not find Message Queue for this topic, " + topic, null);
    }

    public long searchOffset(MessageQueue mq, long timestamp) throws MQClientException {
        // default return lower boundary offset when there are more than one offsets.
        return searchOffset(mq, timestamp, BoundaryType.LOWER);
    }

    public long searchOffset(MessageQueue mq, long timestamp, BoundaryType boundaryType) throws MQClientException {
        String brokerAddr = this.mqClientFactory.findBrokerAddressInPublish(this.mqClientFactory.getBrokerNameFromMessageQueue(mq));
        if (null == brokerAddr) {
            this.mqClientFactory.updateTopicRouteInfoFromNameServer(mq.getTopic());
            brokerAddr = this.mqClientFactory.findBrokerAddressInPublish(this.mqClientFactory.getBrokerNameFromMessageQueue(mq));
        }

        if (brokerAddr != null) {
            try {
                return this.mqClientFactory.getMQClientAPIImpl().searchOffset(brokerAddr, mq, timestamp,
                        boundaryType, timeoutMillis);
            } catch (Exception e) {
                throw new MQClientException("Invoke Broker[" + brokerAddr + "] exception", e);
            }
        }

        throw new MQClientException("The broker[" + mq.getBrokerName() + "] not exist", null);
    }

    public long maxOffset(MessageQueue mq) throws MQClientException {
        String brokerAddr = this.mqClientFactory.findBrokerAddressInPublish(this.mqClientFactory.getBrokerNameFromMessageQueue(mq));
        if (null == brokerAddr) {
            this.mqClientFactory.updateTopicRouteInfoFromNameServer(mq.getTopic());
            brokerAddr = this.mqClientFactory.findBrokerAddressInPublish(this.mqClientFactory.getBrokerNameFromMessageQueue(mq));
        }

        if (brokerAddr != null) {
            try {
                return this.mqClientFactory.getMQClientAPIImpl().getMaxOffset(brokerAddr, mq, timeoutMillis);
            } catch (Exception e) {
                throw new MQClientException("Invoke Broker[" + brokerAddr + "] exception", e);
            }
        }

        throw new MQClientException("The broker[" + mq.getBrokerName() + "] not exist", null);
    }

    public long minOffset(MessageQueue mq) throws MQClientException {
        String brokerAddr = this.mqClientFactory.findBrokerAddressInPublish(this.mqClientFactory.getBrokerNameFromMessageQueue(mq));
        if (null == brokerAddr) {
            this.mqClientFactory.updateTopicRouteInfoFromNameServer(mq.getTopic());
            brokerAddr = this.mqClientFactory.findBrokerAddressInPublish(this.mqClientFactory.getBrokerNameFromMessageQueue(mq));
        }

        if (brokerAddr != null) {
            try {
                return this.mqClientFactory.getMQClientAPIImpl().getMinOffset(brokerAddr, mq, timeoutMillis);
            } catch (Exception e) {
                throw new MQClientException("Invoke Broker[" + brokerAddr + "] exception", e);
            }
        }

        throw new MQClientException("The broker[" + mq.getBrokerName() + "] not exist", null);
    }

    public long earliestMsgStoreTime(MessageQueue mq) throws MQClientException {
        String brokerAddr = this.mqClientFactory.findBrokerAddressInPublish(this.mqClientFactory.getBrokerNameFromMessageQueue(mq));
        if (null == brokerAddr) {
            this.mqClientFactory.updateTopicRouteInfoFromNameServer(mq.getTopic());
            brokerAddr = this.mqClientFactory.findBrokerAddressInPublish(this.mqClientFactory.getBrokerNameFromMessageQueue(mq));
        }

        if (brokerAddr != null) {
            try {
                return this.mqClientFactory.getMQClientAPIImpl().getEarliestMsgStoretime(brokerAddr, mq, timeoutMillis);
            } catch (Exception e) {
                throw new MQClientException("Invoke Broker[" + brokerAddr + "] exception", e);
            }
        }

        throw new MQClientException("The broker[" + mq.getBrokerName() + "] not exist", null);
    }

    public MessageExt viewMessage(String msgId)
        throws RemotingException, MQBrokerException, InterruptedException, MQClientException {
        MessageId messageId = null;
        try {
            messageId = MessageDecoder.decodeMessageId(msgId);
        } catch (Exception e) {
            throw new MQClientException(ResponseCode.NO_MESSAGE, "query message by id finished, but no message.");
        }
        return this.mqClientFactory.getMQClientAPIImpl().viewMessage(NetworkUtil.socketAddress2String(messageId.getAddress()),
            messageId.getOffset(), timeoutMillis);
    }

    public QueryResult queryMessage(String topic, String key, int maxNum, long begin,
        long end) throws MQClientException,
        InterruptedException {
        return queryMessage(topic, key, maxNum, begin, end, false);
    }

    public QueryResult queryMessageByUniqKey(String topic, String uniqKey, int maxNum, long begin, long end)
        throws MQClientException, InterruptedException {

        return queryMessage(topic, uniqKey, maxNum, begin, end, true);
    }

    public MessageExt queryMessageByUniqKey(String topic,
        String uniqKey) throws InterruptedException, MQClientException {
        return queryMessageByUniqKey(topic, uniqKey, System.currentTimeMillis() - 3L * 24 * 60L * 60L * 1000L, Long.MAX_VALUE);
    }

    public MessageExt queryMessageByUniqKey(String clusterName, String topic,
        String uniqKey) throws InterruptedException, MQClientException {
        return queryMessageByUniqKey(clusterName, topic, uniqKey, System.currentTimeMillis() - 3L * 24 * 60L * 60L * 1000L, Long.MAX_VALUE);
    }

    public MessageExt queryMessageByUniqKey(String topic,
        String uniqKey, long begin, long end) throws InterruptedException, MQClientException {
        return queryMessageByUniqKey(null, topic, uniqKey, begin, end);
    }

    public MessageExt queryMessageByUniqKey(String clusterName, String topic,
        String uniqKey, long begin, long end) throws InterruptedException, MQClientException {
        QueryResult qr = this.queryMessage(clusterName, topic, uniqKey, 32, begin, end, true);
        if (qr != null && qr.getMessageList() != null && qr.getMessageList().size() > 0) {
            return qr.getMessageList().get(0);
        } else {
            return null;
        }
    }

    protected QueryResult queryMessage(String topic, String key, int maxNum, long begin, long end,
        boolean isUniqKey) throws MQClientException,
        InterruptedException {
        return queryMessage(null, topic, key, maxNum, begin, end, isUniqKey);
    }

    protected QueryResult queryMessage(String clusterName, String topic, String key, int maxNum, long begin, long end,
        boolean isUniqKey) throws MQClientException,
        InterruptedException {
        TopicRouteData topicRouteData = this.mqClientFactory.getAnExistTopicRouteData(topic);
        if (null == topicRouteData) {
            this.mqClientFactory.updateTopicRouteInfoFromNameServer(topic);
            topicRouteData = this.mqClientFactory.getAnExistTopicRouteData(topic);
        }

        if (topicRouteData != null) {
            List<String> brokerAddrs = new LinkedList<>();
            for (BrokerData brokerData : topicRouteData.getBrokerDatas()) {
                if (clusterName != null && !clusterName.isEmpty()
                    && !clusterName.equals(brokerData.getCluster())) {
                    continue;
                }
                String addr = brokerData.selectBrokerAddr();
                if (addr != null) {
                    brokerAddrs.add(addr);
                }
            }

            if (!brokerAddrs.isEmpty()) {
                final CountDownLatch countDownLatch = new CountDownLatch(brokerAddrs.size());
                final List<QueryResult> queryResultList = new LinkedList<>();
                final ReadWriteLock lock = new ReentrantReadWriteLock(false);

                for (String addr : brokerAddrs) {
                    try {
                        QueryMessageRequestHeader requestHeader = new QueryMessageRequestHeader();
                        requestHeader.setTopic(topic);
                        requestHeader.setKey(key);
                        requestHeader.setMaxNum(maxNum);
                        requestHeader.setBeginTimestamp(begin);
                        requestHeader.setEndTimestamp(end);

                        this.mqClientFactory.getMQClientAPIImpl().queryMessage(addr, requestHeader, timeoutMillis * 3,
                            new InvokeCallback() {
                                @Override
                                public void operationComplete(ResponseFuture responseFuture) {

                                }

                                @Override
                                public void operationSucceed(RemotingCommand response) {
                                    try {
                                        switch (response.getCode()) {
                                            case ResponseCode.SUCCESS: {
                                                QueryMessageResponseHeader responseHeader = null;
                                                try {
                                                    responseHeader =
                                                        (QueryMessageResponseHeader) response
                                                            .decodeCommandCustomHeader(QueryMessageResponseHeader.class);
                                                } catch (RemotingCommandException e) {
                                                    log.error("decodeCommandCustomHeader exception", e);
                                                    return;
                                                }

                                                List<MessageExt> wrappers =
                                                    MessageDecoder.decodes(ByteBuffer.wrap(response.getBody()), true);

                                                QueryResult qr = new QueryResult(responseHeader.getIndexLastUpdateTimestamp(), wrappers);
                                                try {
                                                    lock.writeLock().lock();
                                                    queryResultList.add(qr);
                                                } finally {
                                                    lock.writeLock().unlock();
                                                }
                                                break;
                                            }
                                            default:
                                                log.warn("getResponseCommand failed, {} {}", response.getCode(), response.getRemark());
                                                break;
                                        }

                                    } finally {
                                        countDownLatch.countDown();
                                    }
                                }

                                @Override
                                public void operationFail(Throwable throwable) {
                                    log.error("queryMessage error, requestHeader={}", requestHeader);
                                    countDownLatch.countDown();
                                }
                            }, isUniqKey);
                    } catch (Exception e) {
                        log.warn("queryMessage exception", e);
                    }

                }

                boolean ok = countDownLatch.await(timeoutMillis * 4, TimeUnit.MILLISECONDS);
                if (!ok) {
                    log.warn("queryMessage, maybe some broker failed");
                }

                long indexLastUpdateTimestamp = 0;
                List<MessageExt> messageList = new LinkedList<>();
                for (QueryResult qr : queryResultList) {
                    if (qr.getIndexLastUpdateTimestamp() > indexLastUpdateTimestamp) {
                        indexLastUpdateTimestamp = qr.getIndexLastUpdateTimestamp();
                    }

                    for (MessageExt msgExt : qr.getMessageList()) {
                        if (isUniqKey) {
                            if (msgExt.getMsgId().equals(key)) {
                                messageList.add(msgExt);
                            } else {
                                log.warn("queryMessage by uniqKey, find message key not matched, maybe hash duplicate {}", msgExt.toString());
                            }
                        } else {
                            String keys = msgExt.getKeys();
                            String msgTopic = msgExt.getTopic();
                            if (keys != null) {
                                boolean matched = false;
                                String[] keyArray = keys.split(MessageConst.KEY_SEPARATOR);
                                for (String k : keyArray) {
                                    // both topic and key must be equal at the same time
                                    if (Objects.equals(key, k) && Objects.equals(topic, msgTopic)) {
                                        matched = true;
                                        break;
                                    }
                                }

                                if (matched) {
                                    messageList.add(msgExt);
                                } else {
                                    log.warn("queryMessage, find message key not matched, maybe hash duplicate {}", msgExt.toString());
                                }
                            }
                        }
                    }
                }

                //If namespace not null , reset Topic without namespace.
                if (null != this.mqClientFactory.getClientConfig().getNamespace()) {
                    for (MessageExt messageExt : messageList) {
                        messageExt.setTopic(NamespaceUtil.withoutNamespace(messageExt.getTopic(), this.mqClientFactory.getClientConfig().getNamespace()));
                    }
                }

                if (!messageList.isEmpty()) {
                    return new QueryResult(indexLastUpdateTimestamp, messageList);
                } else {
                    throw new MQClientException(ResponseCode.NO_MESSAGE, "query message by key finished, but no message.");
                }
            }
        }

        throw new MQClientException(ResponseCode.TOPIC_NOT_EXIST, "The topic[" + topic + "] not matched route info");
    }
}
