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
package org.apache.rocketmq.broker.transaction.queue;

import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.client.consumer.PullStatus;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.constant.PermName;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageClientIDSetter;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.heartbeat.SubscriptionData;
import org.apache.rocketmq.common.sysflag.MessageSysFlag;
import org.apache.rocketmq.logging.InnerLoggerFactory;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.store.GetMessageResult;
import org.apache.rocketmq.store.MessageExtBrokerInner;
import org.apache.rocketmq.store.MessageStore;
import org.apache.rocketmq.store.PutMessageResult;
import org.apache.rocketmq.store.PutMessageStatus;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class TransactionalMessageBridge {
    private static final InternalLogger LOGGER = InnerLoggerFactory.getLogger(LoggerName.TRANSACTION_LOGGER_NAME);

    private final ConcurrentHashMap<MessageQueue, MessageQueue> opQueueMap = new ConcurrentHashMap<>();
    private final BrokerController brokerController;
    private final MessageStore store;
    private final SocketAddress storeHost;

    public TransactionalMessageBridge(BrokerController brokerController, MessageStore store) {
        try {
            this.brokerController = brokerController;
            this.store = store;
            this.storeHost =
                new InetSocketAddress(brokerController.getBrokerConfig().getBrokerIP1(),
                    brokerController.getNettyServerConfig().getListenPort());
        } catch (Exception e) {
            LOGGER.error("Init TransactionBridge error", e);
            throw new RuntimeException(e);
        }

    }

    /**
     * 获取MessageQueue对应的offset
     * @param mq
     * @return
     */
    public long fetchConsumeOffset(MessageQueue mq) {
        /**
         * 获取CID_RMQ_SYS_TRANS对应的topic和queueid的消费进度
         *
         * 事务消息   都是以CID_RMQ_SYS_TRANS为消费组来消费
         */
        long offset = brokerController.getConsumerOffsetManager().queryOffset(TransactionalMessageUtil.buildConsumerGroup(),
            mq.getTopic(), mq.getQueueId());
        if (offset == -1) {
            /**
             * 获取最小消费进度
             */
            offset = store.getMinOffsetInQueue(mq.getTopic(), mq.getQueueId());
        }
        return offset;
    }

    /**
     * 获取topic对应的MessageQueue
     * @param topic
     * @return
     */
    public Set<MessageQueue> fetchMessageQueues(String topic) {
        Set<MessageQueue> mqSet = new HashSet<>();
        /**
         * 获取topic对应的配置
         */
        TopicConfig topicConfig = selectTopicConfig(topic);

        /**
         * readQueueNum
         */
        if (topicConfig != null && topicConfig.getReadQueueNums() > 0) {
            for (int i = 0; i < topicConfig.getReadQueueNums(); i++) {
                MessageQueue mq = new MessageQueue();
                mq.setTopic(topic);
                mq.setBrokerName(brokerController.getBrokerConfig().getBrokerName());
                mq.setQueueId(i);
                mqSet.add(mq);
            }
        }
        return mqSet;
    }

    /**
     * 更新mq对应的offset
     * @param mq
     * @param offset
     */
    public void updateConsumeOffset(MessageQueue mq, long offset) {
        /**
         * 指定消费组为CID_RMQ_SYS_TRANS
         */
        this.brokerController.getConsumerOffsetManager().commitOffset(
            RemotingHelper.parseSocketAddressAddr(this.storeHost), TransactionalMessageUtil.buildConsumerGroup(), mq.getTopic(),
            mq.getQueueId(), offset);
    }

    /**
     * 在QueueId下  起始位置为offset  拉取nums条消息   RMQ_SYS_TRANS_HALF_TOPIC
     * @param queueId
     * @param offset
     * @param nums
     * @return
     */
    public PullResult getHalfMessage(int queueId, long offset, int nums) {
        /**
         * CID_SYS_RMQ_TRANS
         */
        String group = TransactionalMessageUtil.buildConsumerGroup();
        /**
         * RMQ_SYS_TRANS_HALF_TOPIC
         */
        String topic = TransactionalMessageUtil.buildHalfTopic();
        SubscriptionData sub = new SubscriptionData(topic, "*");

        /**
         * 拉取消息
         */
        return getMessage(group, topic, queueId, offset, nums, sub);
    }

    /**
     * 在QueueId下  起始位置为offset  拉取nums条消息   RMQ_SYS_TRANS_OP_HALF_TOPIC
     * @param queueId
     * @param offset
     * @param nums
     * @return
     */
    public PullResult getOpMessage(int queueId, long offset, int nums) {
        /**
         * CID_SYS_RMQ_TRANS
         */
        String group = TransactionalMessageUtil.buildConsumerGroup();
        /**
         * RMQ_SYS_TRANS_OP_HALF_TOPIC
         */
        String topic = TransactionalMessageUtil.buildOpTopic();
        SubscriptionData sub = new SubscriptionData(topic, "*");

        /**
         * 拉取消息
         */
        return getMessage(group, topic, queueId, offset, nums, sub);
    }

    /**
     * 拉取消息
     * @param group
     * @param topic
     * @param queueId
     * @param offset
     * @param nums
     * @param sub
     * @return
     */
    private PullResult getMessage(String group, String topic, int queueId, long offset, int nums,
        SubscriptionData sub) {
        /**
         * 拉取消息
         * 拉取消息
         * 拉取消息
         */
        GetMessageResult getMessageResult = store.getMessage(group, topic, queueId, offset, nums, null);

        if (getMessageResult != null) {
            PullStatus pullStatus = PullStatus.NO_NEW_MSG;
            List<MessageExt> foundList = null;
            switch (getMessageResult.getStatus()) {
                /**
                 * 有消息
                 */
                case FOUND:
                    pullStatus = PullStatus.FOUND;
                    /**
                     * 将getMessageResult中的数据解码为消息
                     */
                    foundList = decodeMsgList(getMessageResult);

                    /**
                     * 统计
                     */
                    this.brokerController.getBrokerStatsManager().incGroupGetNums(group, topic,
                        getMessageResult.getMessageCount());
                    this.brokerController.getBrokerStatsManager().incGroupGetSize(group, topic,
                        getMessageResult.getBufferTotalSize());
                    this.brokerController.getBrokerStatsManager().incBrokerGetNums(getMessageResult.getMessageCount());
                    this.brokerController.getBrokerStatsManager().recordDiskFallBehindTime(group, topic, queueId,
                        this.brokerController.getMessageStore().now() - foundList.get(foundList.size() - 1)
                            .getStoreTimestamp());
                    break;
                case NO_MATCHED_MESSAGE:
                    pullStatus = PullStatus.NO_MATCHED_MSG;
                    LOGGER.warn("No matched message. GetMessageStatus={}, topic={}, groupId={}, requestOffset={}",
                        getMessageResult.getStatus(), topic, group, offset);
                    break;
                case NO_MESSAGE_IN_QUEUE:
                    pullStatus = PullStatus.NO_NEW_MSG;
                    LOGGER.warn("No new message. GetMessageStatus={}, topic={}, groupId={}, requestOffset={}",
                        getMessageResult.getStatus(), topic, group, offset);
                    break;
                case MESSAGE_WAS_REMOVING:
                case NO_MATCHED_LOGIC_QUEUE:
                case OFFSET_FOUND_NULL:
                case OFFSET_OVERFLOW_BADLY:
                case OFFSET_OVERFLOW_ONE:
                case OFFSET_TOO_SMALL:
                    pullStatus = PullStatus.OFFSET_ILLEGAL;
                    LOGGER.warn("Offset illegal. GetMessageStatus={}, topic={}, groupId={}, requestOffset={}",
                        getMessageResult.getStatus(), topic, group, offset);
                    break;
                default:
                    assert false;
                    break;
            }

            return new PullResult(pullStatus, getMessageResult.getNextBeginOffset(), getMessageResult.getMinOffset(),
                getMessageResult.getMaxOffset(), foundList);

        } else {
            LOGGER.error("Get message from store return null. topic={}, groupId={}, requestOffset={}", topic, group,
                offset);
            return null;
        }
    }

    /**
     * 将getMessageResult中的ByteBuffer  解码为消息
     * @param getMessageResult
     * @return
     */
    private List<MessageExt> decodeMsgList(GetMessageResult getMessageResult) {
        List<MessageExt> foundList = new ArrayList<>();
        try {
            List<ByteBuffer> messageBufferList = getMessageResult.getMessageBufferList();
            for (ByteBuffer bb : messageBufferList) {
                /**
                 * 解码
                 */
                MessageExt msgExt = MessageDecoder.decode(bb);
                foundList.add(msgExt);
            }

        } finally {
            getMessageResult.release();
        }

        return foundList;
    }

    /**
     * 保存预提交消息   重置发送得topic和queueid
     * @param messageInner
     * @return
     */
    public PutMessageResult putHalfMessage(MessageExtBrokerInner messageInner) {
        return store.putMessage(parseHalfMessageInner(messageInner));
    }

    /**
     * 预提交消息处理  重置发送得topic和queueid
     * @param msgInner
     * @return
     */
    private MessageExtBrokerInner parseHalfMessageInner(MessageExtBrokerInner msgInner) {
        /**
         * 在属性中保存消息真正得topic和queue
         */
        MessageAccessor.putProperty(msgInner, MessageConst.PROPERTY_REAL_TOPIC, msgInner.getTopic());
        MessageAccessor.putProperty(msgInner, MessageConst.PROPERTY_REAL_QUEUE_ID,
            String.valueOf(msgInner.getQueueId()));
        /**
         * 排除回滚类型  增加一般类型
         */
        msgInner.setSysFlag(
            MessageSysFlag.resetTransactionValue(msgInner.getSysFlag(), MessageSysFlag.TRANSACTION_NOT_TYPE));
        /**
         * 改变消息对应得topic和queue   提交到RMQ_SYS_TRANS_HALF_TOPIC中
         */
        msgInner.setTopic(TransactionalMessageUtil.buildHalfTopic());
        msgInner.setQueueId(0);
        msgInner.setPropertiesString(MessageDecoder.messageProperties2String(msgInner.getProperties()));
        return msgInner;
    }

    /**
     * 向RMQ_SYS_TRANS_OP_HALF_TOPIC存入消息
     * @param messageExt
     * @param opType
     * @return
     */
    public boolean putOpMessage(MessageExt messageExt, String opType) {

        MessageQueue messageQueue = new MessageQueue(messageExt.getTopic(),
            this.brokerController.getBrokerConfig().getBrokerName(), messageExt.getQueueId());

        /**
         * 删除
         */
        if (TransactionalMessageUtil.REMOVETAG.equals(opType)) {
            return addRemoveTagInTransactionOp(messageExt, messageQueue);
        }
        return true;
    }

    public PutMessageResult putMessageReturnResult(MessageExtBrokerInner messageInner) {
        LOGGER.debug("[BUG-TO-FIX] Thread:{} msgID:{}", Thread.currentThread().getName(), messageInner.getMsgId());
        return store.putMessage(messageInner);
    }

    /**
     * 存储消息
     * @param messageInner
     * @return
     */
    public boolean putMessage(MessageExtBrokerInner messageInner) {
        PutMessageResult putMessageResult = store.putMessage(messageInner);
        if (putMessageResult != null
            && putMessageResult.getPutMessageStatus() == PutMessageStatus.PUT_OK) {
            return true;
        } else {
            LOGGER.error("Put message failed, topic: {}, queueId: {}, msgId: {}",
                messageInner.getTopic(), messageInner.getQueueId(), messageInner.getMsgId());
            return false;
        }
    }

    public MessageExtBrokerInner renewImmunityHalfMessageInner(MessageExt msgExt) {
        MessageExtBrokerInner msgInner = renewHalfMessageInner(msgExt);
        String queueOffsetFromPrepare = msgExt.getUserProperty(MessageConst.PROPERTY_TRANSACTION_PREPARED_QUEUE_OFFSET);
        if (null != queueOffsetFromPrepare) {
            MessageAccessor.putProperty(msgInner, MessageConst.PROPERTY_TRANSACTION_PREPARED_QUEUE_OFFSET,
                String.valueOf(queueOffsetFromPrepare));
        } else {
            MessageAccessor.putProperty(msgInner, MessageConst.PROPERTY_TRANSACTION_PREPARED_QUEUE_OFFSET,
                String.valueOf(msgExt.getQueueOffset()));
        }

        msgInner.setPropertiesString(MessageDecoder.messageProperties2String(msgInner.getProperties()));

        return msgInner;
    }

    public MessageExtBrokerInner renewHalfMessageInner(MessageExt msgExt) {
        MessageExtBrokerInner msgInner = new MessageExtBrokerInner();
        msgInner.setTopic(msgExt.getTopic());
        msgInner.setBody(msgExt.getBody());
        msgInner.setQueueId(msgExt.getQueueId());
        msgInner.setMsgId(msgExt.getMsgId());
        msgInner.setSysFlag(msgExt.getSysFlag());
        msgInner.setTags(msgExt.getTags());
        msgInner.setTagsCode(MessageExtBrokerInner.tagsString2tagsCode(msgInner.getTags()));
        MessageAccessor.setProperties(msgInner, msgExt.getProperties());
        msgInner.setPropertiesString(MessageDecoder.messageProperties2String(msgExt.getProperties()));
        msgInner.setBornTimestamp(msgExt.getBornTimestamp());
        msgInner.setBornHost(msgExt.getBornHost());
        msgInner.setStoreHost(msgExt.getStoreHost());
        msgInner.setWaitStoreMsgOK(false);
        return msgInner;
    }

    /**
     * RMQ_SYS_TRANS_OP_HALF_TOPIC
     * @param message
     * @param messageQueue
     * @return
     */
    private MessageExtBrokerInner makeOpMessageInner(Message message, MessageQueue messageQueue) {
        MessageExtBrokerInner msgInner = new MessageExtBrokerInner();
        msgInner.setTopic(message.getTopic());
        msgInner.setBody(message.getBody());
        msgInner.setQueueId(messageQueue.getQueueId());
        msgInner.setTags(message.getTags());
        msgInner.setTagsCode(MessageExtBrokerInner.tagsString2tagsCode(msgInner.getTags()));
        msgInner.setSysFlag(0);
        MessageAccessor.setProperties(msgInner, message.getProperties());
        msgInner.setPropertiesString(MessageDecoder.messageProperties2String(message.getProperties()));
        msgInner.setBornTimestamp(System.currentTimeMillis());
        msgInner.setBornHost(this.storeHost);
        msgInner.setStoreHost(this.storeHost);
        msgInner.setWaitStoreMsgOK(false);
        MessageClientIDSetter.setUniqID(msgInner);
        return msgInner;
    }

    /**
     * 获取topic对应的配置  不存在则创建
     * @param topic
     * @return
     */
    private TopicConfig selectTopicConfig(String topic) {
        TopicConfig topicConfig = brokerController.getTopicConfigManager().selectTopicConfig(topic);
        if (topicConfig == null) {
            topicConfig = this.brokerController.getTopicConfigManager().createTopicInSendMessageBackMethod(
                topic, 1, PermName.PERM_WRITE | PermName.PERM_READ, 0);
        }
        return topicConfig;
    }

    /**
     * Use this function while transaction msg is committed or rollback write a flag 'd' to operation queue for the
     * msg's offset
     *
     * @param messageExt Op message
     * @param messageQueue Op message queue
     * @return This method will always return true.
     */
    private boolean addRemoveTagInTransactionOp(MessageExt messageExt, MessageQueue messageQueue) {
        /**
         * RMQ_SYS_TRANS_OP_HALF_TOPIC
         *
         * 消息的body部分  存储得是messageExt的QueueOffset   messageExt对应的topic是RMQ_SYS_TRANS_HALF_TOPIC
         */
        Message message = new Message(TransactionalMessageUtil.buildOpTopic(), TransactionalMessageUtil.REMOVETAG,
            String.valueOf(messageExt.getQueueOffset()).getBytes(TransactionalMessageUtil.charset));

        /**
         * 向RMQ_SYS_TRANS_OP_HALF_TOPIC存入消息
         */
        writeOp(message, messageQueue);
        return true;
    }

    /**
     * 写入RMQ_SYS_TRANS_OP_HALF_TOPIC主题下消息
     * @param message  Message{topic='RMQ_SYS_TRANS_OP_HALF_TOPIC', flag=0, properties={WAIT=true, TAGS=d}, body=[54, 52, 55], transactionId='null'}
     * @param mq       MessageQueue [topic=RMQ_SYS_TRANS_HALF_TOPIC, brokerName=PV-X00204111, queueId=0]
     */
    private void writeOp(Message message, MessageQueue mq) {
        /**
         * opQueue和mq得差别在于topic不同   RMQ_SYS_TRANS_OP_HALF_TOPIC和RMQ_SYS_TRANS_HALF_TOPIC
         */
        MessageQueue opQueue;
        if (opQueueMap.containsKey(mq)) {
            opQueue = opQueueMap.get(mq);
        } else {
            /**
             * 封装RMQ_SYS_TRANS_OP_HALF_TOPIC的消息
             */
            opQueue = getOpQueueByHalf(mq);
            MessageQueue oldQueue = opQueueMap.putIfAbsent(mq, opQueue);
            if (oldQueue != null) {
                opQueue = oldQueue;
            }
        }
        if (opQueue == null) {
            opQueue = new MessageQueue(TransactionalMessageUtil.buildOpTopic(), mq.getBrokerName(), mq.getQueueId());
        }

        /**
         * 封装新消息  RMQ_SYS_TRANS_OP_HALF_TOPIC
         */
        putMessage(makeOpMessageInner(message, opQueue));
    }

    /**
     * 封装RMQ_SYS_TRANS_OP_HALF_TOPIC的消息
     * @param halfMQ
     * @return
     */
    private MessageQueue getOpQueueByHalf(MessageQueue halfMQ) {
        MessageQueue opQueue = new MessageQueue();
        opQueue.setTopic(TransactionalMessageUtil.buildOpTopic());
        opQueue.setBrokerName(halfMQ.getBrokerName());
        opQueue.setQueueId(halfMQ.getQueueId());
        return opQueue;
    }

    /**
     * 获取commitLogOffset处得消息
     * @param commitLogOffset
     * @return
     */
    public MessageExt lookMessageByOffset(final long commitLogOffset) {
        return this.store.lookMessageByOffset(commitLogOffset);
    }

    public BrokerController getBrokerController() {
        return brokerController;
    }
}
