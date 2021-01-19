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
package org.apache.rocketmq.broker.processor;

import com.alibaba.fastjson.JSON;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.util.MsgUtil;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.client.consumer.PullStatus;
import org.apache.rocketmq.common.KeyBuilder;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.PopAckConstants;
import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.TopicFilterType;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.utils.DataConverter;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.store.AppendMessageStatus;
import org.apache.rocketmq.store.GetMessageResult;
import org.apache.rocketmq.store.MessageExtBrokerInner;
import org.apache.rocketmq.store.PutMessageResult;
import org.apache.rocketmq.store.config.BrokerRole;
import org.apache.rocketmq.store.pop.AckMsg;
import org.apache.rocketmq.store.pop.PopCheckPoint;

public class PopReviveService extends ServiceThread {
    private static final InternalLogger POP_LOGGER = InternalLoggerFactory.getLogger(LoggerName.ROCKETMQ_POP_LOGGER_NAME);

    private int queueId;
    private BrokerController brokerController;
    private String reviveTopic;
    private static volatile boolean isMaster = false;

    public PopReviveService(int queueId, BrokerController brokerController, String reviveTopic) {
        super();
        this.queueId = queueId;
        this.brokerController = brokerController;
        this.reviveTopic = reviveTopic;
    }

    @Override
    public String getServiceName() {
        return "PopReviveService_" + this.queueId;
    }

    private boolean checkMaster() {
        return brokerController.getMessageStoreConfig().getBrokerRole() != BrokerRole.SLAVE;
    }

    private boolean checkAndSetMaster() {
        isMaster = checkMaster();
        return isMaster;
    }

    private void reviveRetry(PopCheckPoint popCheckPoint, MessageExt messageExt) throws Exception {
        if (!checkAndSetMaster()) {
            POP_LOGGER.info("slave skip retry , revive topic={}, reviveQueueId={}", reviveTopic, queueId);
            return;
        }
        MessageExtBrokerInner msgInner = new MessageExtBrokerInner();
        if (!popCheckPoint.getT().startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX)) {
            msgInner.setTopic(KeyBuilder.buildPopRetryTopic(popCheckPoint.getT(), popCheckPoint.getC()));
        } else {
            msgInner.setTopic(popCheckPoint.getT());
        }
        msgInner.setBody(messageExt.getBody());
        msgInner.setQueueId(0);
        if (messageExt.getTags() != null) {
            msgInner.setTags(messageExt.getTags());
        } else {
            MessageAccessor.setProperties(msgInner, new HashMap<String, String>());
        }
        msgInner.setBornTimestamp(messageExt.getBornTimestamp());
        msgInner.setBornHost(brokerController.getStoreHost());
        msgInner.setStoreHost(brokerController.getStoreHost());
        msgInner.setReconsumeTimes(messageExt.getReconsumeTimes() + 1);
        msgInner.getProperties().putAll(messageExt.getProperties());
        if (messageExt.getReconsumeTimes() == 0 || msgInner.getProperties().get(MessageConst.PROPERTY_FIRST_POP_TIME) == null) {
            msgInner.getProperties().put(MessageConst.PROPERTY_FIRST_POP_TIME, String.valueOf(popCheckPoint.getPt()));
        }
        msgInner.setPropertiesString(MessageDecoder.messageProperties2String(msgInner.getProperties()));
        addRetryTopicIfNoExit(msgInner.getTopic());
        PutMessageResult putMessageResult = brokerController.getMessageStore().putMessage(msgInner);
        if (brokerController.getBrokerConfig().isEnablePopLog()) {
            POP_LOGGER.info("reviveQueueId={},retry msg , ck={}, msg queueId {}, offset {}, reviveDelay={}, result is {} ",
                    queueId, popCheckPoint, messageExt.getQueueId(), messageExt.getQueueOffset(),
                    (System.currentTimeMillis() - popCheckPoint.getRt()) / 1000, putMessageResult);
        }
        if (putMessageResult.getAppendMessageResult() == null || putMessageResult.getAppendMessageResult().getStatus() != AppendMessageStatus.PUT_OK) {
            throw new Exception("reviveQueueId=" + queueId + ",revive error ,msg is :" + msgInner);
        }
        this.brokerController.getBrokerStatsManager().incBrokerPutNums(1);
        this.brokerController.getBrokerStatsManager().incTopicPutNums(msgInner.getTopic());
        this.brokerController.getBrokerStatsManager().incTopicPutSize(msgInner.getTopic(), putMessageResult.getAppendMessageResult().getWroteBytes());
        if (brokerController.getPopMessageProcessor() != null) {
            brokerController.getPopMessageProcessor().notifyMessageArriving(
                    KeyBuilder.parseNormalTopic(popCheckPoint.getT(), popCheckPoint.getC()),
                    popCheckPoint.getC(),
                    -1
            );
        }
    }

    private boolean addRetryTopicIfNoExit(String topic) {
        TopicConfig topicConfig = brokerController.getTopicConfigManager().selectTopicConfig(topic);
        if (topicConfig != null) {
            return true;
        }
        topicConfig = new TopicConfig(topic);
        topicConfig.setReadQueueNums(PopAckConstants.retryQueueNum);
        topicConfig.setWriteQueueNums(PopAckConstants.retryQueueNum);
        topicConfig.setTopicFilterType(TopicFilterType.SINGLE_TAG);
        topicConfig.setPerm(6);
        topicConfig.setTopicSysFlag(0);
        brokerController.getTopicConfigManager().updateTopicConfig(topicConfig);
        return true;
    }

    private List<MessageExt> getReviveMessage(long offset, int queueId) {
        PullResult pullResult = getMessage(PopAckConstants.REVIVE_GROUP, reviveTopic, queueId, offset, 32);
        if (reachTail(pullResult, offset)) {
            POP_LOGGER.info("reviveQueueId={}, reach tail,offset {}", queueId, offset);
        } else if (pullResult.getPullStatus() == PullStatus.OFFSET_ILLEGAL || pullResult.getPullStatus() == PullStatus.NO_MATCHED_MSG) {
            POP_LOGGER.error("reviveQueueId={}, OFFSET_ILLEGAL {}, result is {}", queueId, offset, pullResult);
            if (!checkAndSetMaster()) {
                POP_LOGGER.info("slave skip offset correct topic={}, reviveQueueId={}", reviveTopic, queueId);
                return null;
            }
            brokerController.getConsumerOffsetManager().commitOffset(PopAckConstants.LOCAL_HOST, PopAckConstants.REVIVE_GROUP, reviveTopic, queueId, pullResult.getNextBeginOffset() - 1);
        }
        return pullResult.getMsgFoundList();
    }

    private boolean reachTail(PullResult pullResult, long offset) {
        return pullResult.getPullStatus() == PullStatus.NO_NEW_MSG
                || (pullResult.getPullStatus() == PullStatus.OFFSET_ILLEGAL && offset == pullResult.getMaxOffset());
    }

    private MessageExt getBizMessage(String topic, long offset, int queueId) {
        final GetMessageResult getMessageTmpResult = brokerController.getMessageStore().getMessage(PopAckConstants.REVIVE_GROUP, topic, queueId, offset, 1, null);
        List<MessageExt> list = decodeMsgList(getMessageTmpResult);
        if (list == null || list.isEmpty()) {
            POP_LOGGER.warn("can not get msg , topic {}, offset {}, queueId {}, result is {}", topic, offset, queueId, getMessageTmpResult);
            return null;
        } else {
            return list.get(0);
        }
    }

    public PullResult getMessage(String group, String topic, int queueId, long offset, int nums) {
        GetMessageResult getMessageResult = brokerController.getMessageStore().getMessage(group, topic, queueId, offset, nums, null);

        if (getMessageResult != null) {
            PullStatus pullStatus = PullStatus.NO_NEW_MSG;
            List<MessageExt> foundList = null;
            switch (getMessageResult.getStatus()) {
                case FOUND:
                    pullStatus = PullStatus.FOUND;
                    foundList = decodeMsgList(getMessageResult);
                    brokerController.getBrokerStatsManager().incGroupGetNums(group, topic, getMessageResult.getMessageCount());
                    brokerController.getBrokerStatsManager().incGroupGetSize(group, topic, getMessageResult.getBufferTotalSize());
                    brokerController.getBrokerStatsManager().incBrokerGetNums(getMessageResult.getMessageCount());
                    brokerController.getBrokerStatsManager().recordDiskFallBehindTime(group, topic, queueId,
                            brokerController.getMessageStore().now() - foundList.get(foundList.size() - 1).getStoreTimestamp());
                    break;
                case NO_MATCHED_MESSAGE:
                    pullStatus = PullStatus.NO_MATCHED_MSG;
                    POP_LOGGER.warn("no matched message. GetMessageStatus={}, topic={}, groupId={}, requestOffset={}",
                            getMessageResult.getStatus(), topic, group, offset);
                    break;
                case NO_MESSAGE_IN_QUEUE:
                    pullStatus = PullStatus.NO_NEW_MSG;
                    POP_LOGGER.warn("no new message. GetMessageStatus={}, topic={}, groupId={}, requestOffset={}",
                            getMessageResult.getStatus(), topic, group, offset);
                    break;
                case MESSAGE_WAS_REMOVING:
                case NO_MATCHED_LOGIC_QUEUE:
                case OFFSET_FOUND_NULL:
                case OFFSET_OVERFLOW_BADLY:
                case OFFSET_OVERFLOW_ONE:
                case OFFSET_TOO_SMALL:
                    pullStatus = PullStatus.OFFSET_ILLEGAL;
                    POP_LOGGER.warn("offset illegal. GetMessageStatus={}, topic={}, groupId={}, requestOffset={}",
                            getMessageResult.getStatus(), topic, group, offset);
                    break;
                default:
                    assert false;
                    break;
            }

            return new PullResult(pullStatus, getMessageResult.getNextBeginOffset(), getMessageResult.getMinOffset(),
                    getMessageResult.getMaxOffset(), foundList);

        } else {
            POP_LOGGER.error("get message from store return null. topic={}, groupId={}, requestOffset={}", topic, group, offset);
            return null;
        }
    }

    private List<MessageExt> decodeMsgList(GetMessageResult getMessageResult) {
        List<MessageExt> foundList = new ArrayList<>();
        try {
            List<ByteBuffer> messageBufferList = getMessageResult.getMessageBufferList();
            if (messageBufferList != null) {
                for (int i = 0; i < messageBufferList.size(); i++) {
                    ByteBuffer bb = messageBufferList.get(i);
                    if (bb == null) {
                        POP_LOGGER.error("bb is null {}", getMessageResult);
                        continue;
                    }
                    MessageExt msgExt = MessageDecoder.decode(bb);
                    if (msgExt == null) {
                        POP_LOGGER.error("decode msgExt is null {}", getMessageResult);
                        continue;
                    }
                    // use CQ offset, not offset in Message
                    msgExt.setQueueOffset(getMessageResult.getMessageQueueOffset().get(i));
                    foundList.add(msgExt);
                }
            }
        } finally {
            getMessageResult.release();
        }

        return foundList;
    }

    private void consumeReviveMessage(ConsumeReviveObj consumeReviveObj) {
        HashMap<String, PopCheckPoint> map = consumeReviveObj.map;
        long startScanTime = System.currentTimeMillis();
        long endTime = 0;
        long oldOffset = brokerController.getConsumerOffsetManager().queryOffset(PopAckConstants.REVIVE_GROUP, reviveTopic, queueId);
        consumeReviveObj.oldOffset = oldOffset;
        POP_LOGGER.info("reviveQueueId={}, old offset is {} ", queueId, oldOffset);
        long offset = oldOffset + 1;
        long firstRt = 0;
        // offset self amend
        while (true) {
            if (!checkAndSetMaster()) {
                POP_LOGGER.info("slave skip scan , revive topic={}, reviveQueueId={}", reviveTopic, queueId);
                break;
            }
            List<MessageExt> messageExts = getReviveMessage(offset, queueId);
            if (messageExts == null || messageExts.isEmpty()) {
                break;
            }
            if (System.currentTimeMillis() - startScanTime > brokerController.getBrokerConfig().getReviveScanTime()) {
                POP_LOGGER.info("reviveQueueId={}, scan timeout  ", queueId);
                break;
            }
            for (MessageExt messageExt : messageExts) {
                if (PopAckConstants.CK_TAG.equals(messageExt.getTags())) {
                    String raw = new String(messageExt.getBody(), DataConverter.charset);
                    if (brokerController.getBrokerConfig().isEnablePopLog()) {
                        POP_LOGGER.info("reviveQueueId={},find ck, offset:{}, raw : {}", messageExt.getQueueId(), messageExt.getQueueOffset(), raw);
                    }
                    PopCheckPoint point = JSON.parseObject(raw, PopCheckPoint.class);
                    if (point.getT() == null || point.getC() == null) {
                        continue;
                    }
                    map.put(point.getT() + point.getC() + point.getQ() + point.getSo() + point.getPt(), point);
                    point.setRo(messageExt.getQueueOffset());
                    if (firstRt == 0) {
                        firstRt = point.getRt();
                    }
                } else if (PopAckConstants.ACK_TAG.equals(messageExt.getTags())) {
                    String raw = new String(messageExt.getBody(), DataConverter.charset);
                    if (brokerController.getBrokerConfig().isEnablePopLog()) {
                        POP_LOGGER.info("reviveQueueId={},find ack, offset:{}, raw : {}", messageExt.getQueueId(), messageExt.getQueueOffset(), raw);
                    }
                    AckMsg ackMsg = JSON.parseObject(raw, AckMsg.class);
                    PopCheckPoint point = map.get(ackMsg.getT() + ackMsg.getC() + ackMsg.getQ() + ackMsg.getSo() + ackMsg.getPt());
                    if (point == null) {
                        continue;
                    }
                    int indexOfAck = point.indexOfAck(ackMsg.getAo());
                    if (indexOfAck > -1) {
                        point.setBm(DataConverter.setBit(point.getBm(), indexOfAck, true));
                    } else {
                        POP_LOGGER.error("invalid ack index, {}, {}", ackMsg, point);
                    }
                }
                long deliverTime = MsgUtil.getMessageDeliverTime(this.brokerController, messageExt);
                if (deliverTime > endTime) {
                    endTime = deliverTime;
                }
            }
            offset = offset + messageExts.size();
        }
        consumeReviveObj.endTime = endTime;
    }

    private void mergeAndRevive(ConsumeReviveObj consumeReviveObj) throws Throwable {
        ArrayList<PopCheckPoint> sortList = consumeReviveObj.genSortList();
        POP_LOGGER.info("reviveQueueId={},ck listSize={}", queueId, sortList.size());
        if (sortList.size() != 0) {
            POP_LOGGER.info("reviveQueueId={}, 1st ck, startOffset={}, reviveOffset={} ; last ck, startOffset={}, reviveOffset={}", queueId, sortList.get(0).getSo(),
                    sortList.get(0).getRo(), sortList.get(sortList.size() - 1).getSo(), sortList.get(sortList.size() - 1).getRo());
        }
        long newOffset = consumeReviveObj.oldOffset;
        for (PopCheckPoint popCheckPoint : sortList) {
            if (!checkAndSetMaster()) {
                POP_LOGGER.info("slave skip ck process , revive topic={}, reviveQueueId={}", reviveTopic, queueId);
                break;
            }
            if (consumeReviveObj.endTime - popCheckPoint.getRt() <= (PopAckConstants.ackTimeInterval + PopAckConstants.SECOND)) {
                break;
            }

            // check normal topic, skip ck , if normal topic is not exist
            String normalTopic = KeyBuilder.parseNormalTopic(popCheckPoint.getT(), popCheckPoint.getC());
            if (brokerController.getTopicConfigManager().selectTopicConfig(normalTopic) == null) {
                POP_LOGGER.warn("reviveQueueId={},can not get normal topic {} , then continue ", queueId, popCheckPoint.getT());
                newOffset = popCheckPoint.getRo();
                continue;
            }
            if (null == brokerController.getSubscriptionGroupManager().findSubscriptionGroupConfig(popCheckPoint.getC())) {
                POP_LOGGER.warn("reviveQueueId={},can not get cid {} , then continue ", queueId, popCheckPoint.getC());
                newOffset = popCheckPoint.getRo();
                continue;
            }

            reviveMsgFromCk(popCheckPoint);

            newOffset = popCheckPoint.getRo();
        }
        if (newOffset > consumeReviveObj.oldOffset) {
            if (!checkAndSetMaster()) {
                POP_LOGGER.info("slave skip commit, revive topic={}, reviveQueueId={}", reviveTopic, queueId);
                return;
            }
            brokerController.getConsumerOffsetManager().commitOffset(PopAckConstants.LOCAL_HOST, PopAckConstants.REVIVE_GROUP, reviveTopic, queueId, newOffset);
        }
        consumeReviveObj.newOffset = newOffset;
    }

    private void reviveMsgFromCk(PopCheckPoint popCheckPoint) throws Throwable {
        for (int j = 0; j < popCheckPoint.getN(); j++) {
            if (DataConverter.getBit(popCheckPoint.getBm(), j)) {
                continue;
            }

            // retry msg
            long msgOffset = popCheckPoint.ackOffsetByIndex((byte) j);
            MessageExt messageExt = getBizMessage(popCheckPoint.getT(), msgOffset, popCheckPoint.getQ());
            if (messageExt == null) {
                POP_LOGGER.warn("reviveQueueId={},can not get biz msg topic is {}, offset is {} , then continue ",
                        queueId, popCheckPoint.getT(), msgOffset);
                continue;
            }
            //skip ck from last epoch
            if (popCheckPoint.getPt() < messageExt.getStoreTimestamp()) {
                POP_LOGGER.warn("reviveQueueId={},skip ck from last epoch {}", queueId, popCheckPoint);
                continue;
            }
            reviveRetry(popCheckPoint, messageExt);
        }
    }

    @Override
    public void run() {
        int slow = 1;
        while (!this.isStopped()) {
            try {
                if (System.currentTimeMillis() < brokerController.getShouldStartTime()) {
                    POP_LOGGER.info("PopReviveService Ready to run after {}", brokerController.getShouldStartTime());
                    this.waitForRunning(1000);
                    continue;
                }
                this.waitForRunning(brokerController.getBrokerConfig().getReviveInterval());
                if (!checkAndSetMaster()) {
                    POP_LOGGER.info("slave skip start revive topic={}, reviveQueueId={}", reviveTopic, queueId);
                    continue;
                }

                POP_LOGGER.info("start revive topic={}, reviveQueueId={}", reviveTopic, queueId);
                ConsumeReviveObj consumeReviveObj = new ConsumeReviveObj();
                consumeReviveMessage(consumeReviveObj);

                if (!checkAndSetMaster()) {
                    POP_LOGGER.info("slave skip scan , revive topic={}, reviveQueueId={}", reviveTopic, queueId);
                    continue;
                }

                mergeAndRevive(consumeReviveObj);

                ArrayList<PopCheckPoint> sortList = consumeReviveObj.sortList;
                long delay = 0;
                if (sortList != null && !sortList.isEmpty()) {
                    delay = (System.currentTimeMillis() - sortList.get(0).getRt()) / 1000;
                    slow = 1;
                }

                POP_LOGGER.info("reviveQueueId={},revive finish,old offset is {}, new offset is {}, ckDelay={}  ",
                        queueId, consumeReviveObj.oldOffset, consumeReviveObj.newOffset, delay);

                if (sortList == null || sortList.isEmpty()) {
                    POP_LOGGER.info("reviveQueueId={},has no new msg ,take a rest {}", queueId, slow);
                    this.waitForRunning(slow * brokerController.getBrokerConfig().getReviveInterval());
                    if (slow < brokerController.getBrokerConfig().getReviveMaxSlow()) {
                        slow++;
                    }
                }

            } catch (Throwable e) {
                POP_LOGGER.error("reviveQueueId=" + queueId + ",revive error", e);
            }
        }
    }

    static class ConsumeReviveObj {
        HashMap<String, PopCheckPoint> map = new HashMap<>();
        ArrayList<PopCheckPoint> sortList;
        long oldOffset;
        long endTime;
        long newOffset;

        ArrayList<PopCheckPoint> genSortList() {
            if (sortList != null) {
                return sortList;
            }
            sortList = new ArrayList<>(map.values());
            Collections.sort(sortList, new Comparator<PopCheckPoint>() {
                @Override
                public int compare(PopCheckPoint o1, PopCheckPoint o2) {
                    return (int) (o1.getRo() - o2.getRo());
                }
            });
            return sortList;
        }
    }
}
