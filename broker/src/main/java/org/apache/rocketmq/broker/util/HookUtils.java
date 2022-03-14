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
package org.apache.rocketmq.broker.util;

import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.schedule.ScheduleMessageService;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageExtBrokerInner;
import org.apache.rocketmq.common.sysflag.MessageSysFlag;
import org.apache.rocketmq.common.topic.TopicValidator;
import org.apache.rocketmq.common.utils.QueueTypeUtils;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.store.PutMessageResult;
import org.apache.rocketmq.store.PutMessageStatus;
import org.apache.rocketmq.store.config.BrokerRole;

public class HookUtils {

    protected static final InternalLogger LOG = InternalLoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);

    private static AtomicLong printTimes = new AtomicLong(0);

    public static PutMessageResult checkBeforePutMessage(BrokerController brokerController, final MessageExt msg) {
        if (brokerController.getMessageStore().isShutdown()) {
            LOG.warn("message store has shutdown, so putMessage is forbidden");
            return new PutMessageResult(PutMessageStatus.SERVICE_NOT_AVAILABLE, null);
        }

        if (!brokerController.getMessageStoreConfig().isDuplicationEnable() && BrokerRole.SLAVE == brokerController.getMessageStoreConfig().getBrokerRole()) {
            long value = printTimes.getAndIncrement();
            if ((value % 50000) == 0) {
                LOG.warn("message store is in slave mode, so putMessage is forbidden ");
            }

            return new PutMessageResult(PutMessageStatus.SERVICE_NOT_AVAILABLE, null);
        }

        if (!brokerController.getMessageStore().getRunningFlags().isWriteable()) {
            long value = printTimes.getAndIncrement();
            if ((value % 50000) == 0) {
                LOG.warn("message store is not writeable, so putMessage is forbidden " + brokerController.getMessageStore().getRunningFlags().getFlagBits());
            }

            return new PutMessageResult(PutMessageStatus.SERVICE_NOT_AVAILABLE, null);
        } else {
            printTimes.set(0);
        }

        final byte[] topicData = msg.getTopic().getBytes(MessageDecoder.CHARSET_UTF8);
        final int topicLength = topicData == null ? 0 : topicData.length;

        if (topicLength > brokerController.getMessageStoreConfig().getMaxTopicLength()) {
            LOG.warn("putMessage message topic[{}] length too long {}", msg.getTopic(), topicLength);
            return new PutMessageResult(PutMessageStatus.MESSAGE_ILLEGAL, null);
        }

        if (topicLength > Byte.MAX_VALUE) {
            LOG.warn("putMessage message topic[{}] length too long {}, but it is not supported by broker",
                msg.getTopic(), topicLength);
            return new PutMessageResult(PutMessageStatus.MESSAGE_ILLEGAL, null);
        }

        if (msg.getBody() == null) {
            LOG.warn("putMessage message topic[{}], but message body is null", msg.getTopic());
            return new PutMessageResult(PutMessageStatus.MESSAGE_ILLEGAL, null);
        }

        if (msg.getBody() == null) {
            LOG.warn("putMessage message topic[{}], but message body is null", msg.getTopic());
            return new PutMessageResult(PutMessageStatus.MESSAGE_ILLEGAL, null);
        }

        if (brokerController.getMessageStore().isOSPageCacheBusy()) {
            return new PutMessageResult(PutMessageStatus.OS_PAGE_CACHE_BUSY, null);
        }
        return null;
    }

    public static PutMessageResult checkInnerBatch(BrokerController brokerController, final MessageExt msg) {
        if (msg.getProperties().containsKey(MessageConst.PROPERTY_INNER_NUM)
            && !MessageSysFlag.check(msg.getSysFlag(), MessageSysFlag.INNER_BATCH_FLAG)) {
            LOG.warn("[BUG]The message had property {} but is not an inner batch", MessageConst.PROPERTY_INNER_NUM);
            return new PutMessageResult(PutMessageStatus.MESSAGE_ILLEGAL, null);
        }

        if (MessageSysFlag.check(msg.getSysFlag(), MessageSysFlag.INNER_BATCH_FLAG)) {
            Optional<TopicConfig> topicConfig = Optional.ofNullable(brokerController.getTopicConfigManager().getTopicConfigTable().get(msg.getTopic()));
            if (!QueueTypeUtils.isBatchCq(topicConfig)) {
                LOG.error("[BUG]The message is an inner batch but cq type is not batch cq");
                return new PutMessageResult(PutMessageStatus.MESSAGE_ILLEGAL, null);
            }
        }

        return null;
    }

    public static PutMessageResult handleScheduleMessage(BrokerController brokerController,
        final MessageExtBrokerInner msg) {
        final int tranType = MessageSysFlag.getTransactionValue(msg.getSysFlag());
        if (tranType == MessageSysFlag.TRANSACTION_NOT_TYPE
            || tranType == MessageSysFlag.TRANSACTION_COMMIT_TYPE) {
            if (!TopicValidator.RMQ_SYS_SCHEDULE_TOPIC.equals(msg.getTopic()) && msg.getDelayTimeLevel() > 0) {
                transformDelayLevelMessage(brokerController, msg);
            }
        }
        return null;
    }

    public static void transformDelayLevelMessage(BrokerController brokerController, MessageExtBrokerInner msg) {

        if (msg.getDelayTimeLevel() > brokerController.getScheduleMessageService().getMaxDelayLevel()) {
            msg.setDelayTimeLevel(brokerController.getScheduleMessageService().getMaxDelayLevel());
        }

        // Backup real topic, queueId
        MessageAccessor.putProperty(msg, MessageConst.PROPERTY_REAL_TOPIC, msg.getTopic());
        MessageAccessor.putProperty(msg, MessageConst.PROPERTY_REAL_QUEUE_ID, String.valueOf(msg.getQueueId()));
        msg.setPropertiesString(MessageDecoder.messageProperties2String(msg.getProperties()));

        msg.setTopic(TopicValidator.RMQ_SYS_SCHEDULE_TOPIC);
        msg.setQueueId(ScheduleMessageService.delayLevel2QueueId(msg.getDelayTimeLevel()));
    }

    public static boolean sendMessageBack(BrokerController brokerController, List<MessageExt> msgList,
        String brokerName, String brokerAddr) {
        try {
            Iterator<MessageExt> it = msgList.iterator();
            while (it.hasNext()) {
                MessageExt msg = it.next();
                msg.setWaitStoreMsgOK(false);
                brokerController.getBrokerOuterAPI().sendMessageToSpecificBroker(brokerAddr, brokerName, msg, "InnerSendMessageBackGroup", 3000);
                it.remove();
            }
        } catch (Exception e) {
            LOG.error("send message back to broker {} addr {} failed", brokerName, brokerAddr, e);
            return false;
        }
        return true;
    }
}
