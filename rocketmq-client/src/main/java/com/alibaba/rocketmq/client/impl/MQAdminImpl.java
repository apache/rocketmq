/**
 * $Id: MQAdminImpl.java 1831 2013-05-16 01:39:51Z shijia.wxr $
 */
package com.alibaba.rocketmq.client.impl;

import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import com.alibaba.rocketmq.client.QueryResult;
import com.alibaba.rocketmq.client.exception.MQBrokerException;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.client.impl.factory.MQClientFactory;
import com.alibaba.rocketmq.client.impl.producer.TopicPublishInfo;
import com.alibaba.rocketmq.common.MixAll;
import com.alibaba.rocketmq.common.TopicConfig;
import com.alibaba.rocketmq.common.TopicFilterType;
import com.alibaba.rocketmq.common.message.Message;
import com.alibaba.rocketmq.common.message.MessageDecoder;
import com.alibaba.rocketmq.common.message.MessageExt;
import com.alibaba.rocketmq.common.message.MessageId;
import com.alibaba.rocketmq.common.message.MessageQueue;
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
import com.alibaba.rocketmq.remoting.protocol.RemotingProtos.ResponseCode;


/**
 * 管理类接口实现
 * 
 * @author shijia.wxr<vintage.wang@gmail.com>
 * 
 */
public class MQAdminImpl {
    private final MQClientFactory mQClientFactory;


    public MQAdminImpl(MQClientFactory mQClientFactory) {
        this.mQClientFactory = mQClientFactory;
    }


    public void createTopic(String key, String newTopic, int queueNum, TopicFilterType topicFilterType,
            boolean order) throws MQClientException {
        try {
            TopicRouteData topicRouteData =
                    this.mQClientFactory.getMQClientAPIImpl().getTopicRouteInfoFromNameServer(key, 1000 * 3);
            List<BrokerData> brokerDataList = topicRouteData.getBrokerDatas();
            if (brokerDataList != null && !brokerDataList.isEmpty()) {
                // 排序原因：即使没有配置顺序消息模式，默认队列的顺序同配置的一致。
                Collections.sort(brokerDataList);

                MQClientException exception = null;

                StringBuilder orderTopicString = new StringBuilder();

                // 遍历各个Broker
                for (BrokerData brokerData : brokerDataList) {
                    String addr = brokerData.getBrokerAddrs().get(MixAll.MASTER_ID);
                    if (addr != null) {
                        TopicConfig topicConfig = new TopicConfig(newTopic);
                        topicConfig.setReadQueueNums(queueNum);
                        topicConfig.setWriteQueueNums(queueNum);
                        topicConfig.setTopicFilterType(topicFilterType);
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

                if (order) {
                    // 向Name Server注册顺序消息
                    this.mQClientFactory.getMQClientAPIImpl().registerOrderTopic(newTopic,
                        orderTopicString.toString(), 1000 * 10);
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
                    this.mQClientFactory.getMQClientAPIImpl().getTopicRouteInfoFromNameServer(topic, 1000 * 3);
            if (topicRouteData != null) {
                TopicPublishInfo topicPublishInfo =
                        MQClientFactory.topicRouteData2TopicPublishInfo(topic, topicRouteData);
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


    public List<MessageQueue> fetchSubscribeMessageQueues(String topic) throws MQClientException {
        try {
            TopicRouteData topicRouteData =
                    this.mQClientFactory.getMQClientAPIImpl().getTopicRouteInfoFromNameServer(topic, 1000 * 3);
            if (topicRouteData != null) {
                List<MessageQueue> mqList =
                        MQClientFactory.topicRouteData2TopicSubscribeInfo(topic, topicRouteData);
                if (!mqList.isEmpty()) {
                    return mqList;
                }
                else {
                    throw new MQClientException("Can not find Message Queue for this topic, " + topic, null);
                }
            }
        }
        catch (Exception e) {
            throw new MQClientException("Can not find Message Queue for this topic, " + topic, e);
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


    public long getMaxOffset(MessageQueue mq) throws MQClientException {
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


    public long getMinOffset(MessageQueue mq) throws MQClientException {
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


    public long getEarliestMsgStoreTime(MessageQueue mq) throws MQClientException {
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
            return this.mQClientFactory.getMQClientAPIImpl()
                .viewMessage(RemotingUtil.socketAddress2String(messageId.getAddress()),
                    messageId.getOffset(), 1000 * 3);
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
                String addr = brokerData.getOneBrokerAddr();
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

                        this.mQClientFactory.getMQClientAPIImpl().queryMessage(addr, requestHeader, 1000 * 5,
                            new InvokeCallback() {
                                @Override
                                public void operationComplete(ResponseFuture responseFuture) {
                                    countDownLatch.countDown();
                                    RemotingCommand response = responseFuture.getResponseCommand();
                                    if (response != null) {
                                        switch (response.getCode()) {
                                        case ResponseCode.SUCCESS_VALUE: {
                                            QueryMessageResponseHeader responseHeader = null;
                                            try {
                                                responseHeader =
                                                        (QueryMessageResponseHeader) response
                                                            .decodeCommandCustomHeader(QueryMessageResponseHeader.class);
                                            }
                                            catch (RemotingCommandException e) {
                                                // TODO log
                                                return;
                                            }

                                            List<MessageExt> wrappers =
                                                    MessageDecoder.decodes(
                                                        ByteBuffer.wrap(response.getBody()), true);

                                            QueryResult qr =
                                                    new QueryResult(responseHeader.getIndexLastUpdateTimestamp(),
                                                        wrappers);
                                            queryResultList.add(qr);
                                            break;
                                        }
                                        default:
                                            // TODO error log
                                            break;
                                        }
                                    }
                                    else {
                                        // TODO log
                                    }
                                }
                            });
                    }
                    catch (RemotingException e) {
                        // TODO log
                    }
                    catch (MQBrokerException e) {
                        // TODO log
                    }
                    catch (InterruptedException e) {
                        // TODO log
                    } // end of try
                } // end of for

                boolean ok = countDownLatch.await(1000 * 10, TimeUnit.MILLISECONDS);
                if (!ok) {
                    // TODO log，仅仅打印日志即可，有可能部分成功，部分超时
                }

                long indexLastUpdateTimestamp = 0;
                List<MessageExt> messageList = new LinkedList<MessageExt>();
                for (QueryResult qr : queryResultList) {
                    if (qr.getIndexLastUpdateTimestamp() > indexLastUpdateTimestamp) {
                        indexLastUpdateTimestamp = qr.getIndexLastUpdateTimestamp();
                    }

                    for (MessageExt wrapper : qr.getMessageList()) {
                        String keys = wrapper.getKeys();
                        if (keys != null) {
                            boolean matched = false;
                            String[] keyArray = keys.split(Message.KEY_SEPARATOR);
                            if (keyArray != null) {
                                for (String k : keyArray) {
                                    if (key.equals(k)) {
                                        matched = true;
                                        break;
                                    }
                                }
                            }

                            if (matched) {
                                messageList.add(wrapper);
                            }
                            else {
                                // TODO log
                            }
                        }
                    }
                }

                if (!messageList.isEmpty()) {
                    return new QueryResult(indexLastUpdateTimestamp, messageList);
                }
                else {
                    // TODO log
                    throw new MQClientException("query operation over, but no message.", null);
                }
            }
        }

        throw new MQClientException("The topic[" + topic + "] not matched route info", null);
    }
}
