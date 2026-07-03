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
import org.apache.rocketmq.common.MixAll;
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
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.store.PutMessageResult;
import org.apache.rocketmq.store.PutMessageStatus;
import org.apache.rocketmq.store.config.BrokerRole;
import org.apache.rocketmq.store.hook.SendMessageBackHook;
import org.apache.rocketmq.store.queue.ConsumeQueueStoreInterface;
import org.apache.rocketmq.store.timer.TimerMessageStore;

/**
 * Pre-processing utilities invoked before putting a message to the store.
 *
 * <p>All methods are static and called sequentially from
 * {@code SendMessageProcessor#asyncSendMessage}:
 * <ol>
 *   <li>{@link #checkBeforePutMessage} — validates store state, topic length,
 *       body presence, and OS page cache pressure</li>
 *   <li>{@link #checkInnerBatch} — checks inner-batch sysFlag consistency</li>
 *   <li>{@link #handleScheduleMessage} — routes timer and delay-level messages
 *       to {@code TIMER_TOPIC} or {@code SCHEDULE_TOPIC}</li>
 *   <li>{@link #handleLmqQuota} — enforces Light Message Queue limits</li>
 * </ol>
 *
 * <p>If any step returns a non-null {@link PutMessageResult}, the operation is
 * aborted immediately.
 */
public class HookUtils {

    protected static final Logger LOG = LoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);

    private static final AtomicLong PRINT_TIMES = new AtomicLong(0);

    /**
     * On Linux: The maximum length for a file name is 255 bytes.
     * The maximum combined length of both the file name and path name is 4096 bytes.
     * This length matches the PATH_MAX that is supported by the operating system.
     * The Unicode representation of a character can occupy several bytes,
     * so the maximum number of characters that comprises a path and file name can vary.
     * The actual limitation is the number of bytes in the path and file components,
     * which might correspond to an equal number of characters.
     */
    private static final Integer MAX_TOPIC_LENGTH = 255;

    /**
     * Pre-put message validation: guards against writes when the store is
     * shut down, in slave mode (non-duplication), not writable, topic too long,
     * body null, or OS page cache busy.
     *
     * @return null if the check passes, or a rejection {@link PutMessageResult}
     */
    public static PutMessageResult checkBeforePutMessage(BrokerController brokerController, final MessageExt msg) {
        if (brokerController.getMessageStore().isShutdown()) {
            LOG.warn("message store has shutdown, so putMessage is forbidden");
            return new PutMessageResult(PutMessageStatus.SERVICE_NOT_AVAILABLE, null);
        }

        if (!brokerController.getMessageStoreConfig().isDuplicationEnable() && BrokerRole.SLAVE == brokerController.getMessageStoreConfig().getBrokerRole()) {
            long value = PRINT_TIMES.getAndIncrement();
            if ((value % 50000) == 0) {
                LOG.warn("message store is in slave mode, so putMessage is forbidden ");
            }

            return new PutMessageResult(PutMessageStatus.SERVICE_NOT_AVAILABLE, null);
        }

        if (!brokerController.getMessageStore().getRunningFlags().isWriteable()) {
            long value = PRINT_TIMES.getAndIncrement();
            if ((value % 50000) == 0) {
                LOG.warn("message store is not writeable, so putMessage is forbidden " + brokerController.getMessageStore().getRunningFlags().getFlagBits());
            }

            return new PutMessageResult(PutMessageStatus.SERVICE_NOT_AVAILABLE, null);
        } else {
            PRINT_TIMES.set(0);
        }

        final byte[] topicData = msg.getTopic().getBytes(MessageDecoder.CHARSET_UTF8);
        boolean retryTopic = msg.getTopic() != null && msg.getTopic().startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX);
        if (!retryTopic && topicData.length > Byte.MAX_VALUE) {
            LOG.warn("putMessage message topic[{}] length too long {}, but it is not supported by broker",
                msg.getTopic(), topicData.length);
            return new PutMessageResult(PutMessageStatus.MESSAGE_ILLEGAL, null);
        }

        if (topicData.length > MAX_TOPIC_LENGTH) {
            LOG.warn("putMessage message topic[{}] length too long {}, but it is not supported by broker",
                msg.getTopic(), topicData.length);
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

    /**
     * Check inner-batch sysFlag consistency
     * There is no inner-batch after v5.0.0
     *
     * @param brokerController brokerController(object container)
     * @param msg msg
     * @return putMessageResult
     */
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

    /**
     * Route timer or delay-level messages to the appropriate system topic.
     *
     * <p>For non-transaction or committed messages, two checks run in order:
     * <ol>
     *   <li><b>Timer wheel</b> — if the message carries timer properties
     *   ({@code PROPERTY_TIMER_DELIVER_MS}, etc.), it is transformed and
     *   redirected to {@code TIMER_TOPIC}. The TimerWheel must be enabled,
     *   otherwise the message is rejected.</li>
     *   <li><b>Delay level</b> — if {@code delayTimeLevel > 0}, the message
     *   is redirected to {@code SCHEDULE_TOPIC_XXXX}. Both checks can apply
     *   to the same message (legacy bridge).</li>
     * </ol>
     *
     * @return non-null {@link PutMessageResult} if the message was rejected
     */
    public static PutMessageResult handleScheduleMessage(BrokerController brokerController,
        final MessageExtBrokerInner msg) {
        final int tranType = MessageSysFlag.getTransactionValue(msg.getSysFlag());
        // normal message or committed message can be delayed
        if (tranType == MessageSysFlag.TRANSACTION_NOT_TYPE
            || tranType == MessageSysFlag.TRANSACTION_COMMIT_TYPE) {
            // is timer topic
            if (!isRolledTimerMessage(msg)) {
                // double check, has delay level or, is timer topic and has delivery time
                if (checkIfTimerMessage(msg)) {
                    if (!brokerController.getMessageStoreConfig().isTimerWheelEnable()) {
                        //wheel timer is not enabled, reject the message
                        return new PutMessageResult(PutMessageStatus.WHEEL_TIMER_NOT_ENABLE, null);
                    }
                    PutMessageResult transformRes = transformTimerMessage(brokerController, msg);
                    if (null != transformRes) {
                        return transformRes;
                    }
                }
            }
            // Delay Delivery, useless with default config
            if (msg.getDelayTimeLevel() > 0) {
                transformDelayLevelMessage(brokerController, msg);
            }
        }
        return null;
    }

    /**
     * Enforce Light Message Queue (LMQ) quota.
     * reject the message if:
     *  - the number of LMQ consume queues would exceed the configured maximum
     *  - and the target queue does not already exist.
     *
     * @return null if the check passes, or a rejection {@link PutMessageResult}
     */
    public static PutMessageResult handleLmqQuota(BrokerController brokerController, final MessageExtBrokerInner msg) {
        if (!brokerController.getMessageStoreConfig().isEnableLmqQuota()
            || !brokerController.getMessageStoreConfig().isEnableLmq()
            || !brokerController.getMessageStoreConfig().isEnableMultiDispatch()
            || !msg.needDispatchLMQ()) {
            return null;
        }

        ConsumeQueueStoreInterface cqStore = brokerController.getMessageStore().getQueueStore();
        String[] queueNames =
            msg.getProperty(MessageConst.PROPERTY_INNER_MULTI_DISPATCH).split(MixAll.LMQ_DISPATCH_SEPARATOR);
        for (String queueName : queueNames) {
            // starts with LMQ_PREFIX(%LMQ%)
            if (!MixAll.isLmq(queueName)) {
                continue;
            }
            if (cqStore.getLmqNum() >= brokerController.getMessageStoreConfig().getMaxLmqConsumeQueueNum()) {
                if (!cqStore.isLmqExist(queueName)) {
                    return new PutMessageResult(PutMessageStatus.LMQ_CONSUME_QUEUE_NUM_EXCEEDED, null);
                }
            }
        }
        return null;
    }

    private static boolean isRolledTimerMessage(MessageExtBrokerInner msg) {
        return TimerMessageStore.TIMER_TOPIC.equals(msg.getTopic());
    }

    public static boolean checkIfTimerMessage(MessageExtBrokerInner msg) {
        if (msg.getDelayTimeLevel() > 0) {
            if (null != msg.getProperty(MessageConst.PROPERTY_TIMER_DELIVER_MS)) {
                MessageAccessor.clearProperty(msg, MessageConst.PROPERTY_TIMER_DELIVER_MS);
            }
            if (null != msg.getProperty(MessageConst.PROPERTY_TIMER_DELAY_SEC)) {
                MessageAccessor.clearProperty(msg, MessageConst.PROPERTY_TIMER_DELAY_SEC);
            }
            if (null != msg.getProperty(MessageConst.PROPERTY_TIMER_DELAY_MS)) {
                MessageAccessor.clearProperty(msg, MessageConst.PROPERTY_TIMER_DELAY_MS);
            }
            return false;
            //return this.defaultMessageStore.getMessageStoreConfig().isTimerInterceptDelayLevel();
        }
        //double check
        if (TimerMessageStore.TIMER_TOPIC.equals(msg.getTopic()) || null != msg.getProperty(MessageConst.PROPERTY_TIMER_OUT_MS)) {
            return false;
        }
        return null != msg.getProperty(MessageConst.PROPERTY_TIMER_DELIVER_MS) || null != msg.getProperty(MessageConst.PROPERTY_TIMER_DELAY_MS) || null != msg.getProperty(MessageConst.PROPERTY_TIMER_DELAY_SEC);
    }

    /**
     * Transform a timer message and redirect it to the timer wheel topic.
     *
     * <p>Parses the delivery time from {@code PROPERTY_TIMER_DELAY_SEC},
     * {@code PROPERTY_TIMER_DELAY_MS}, or {@code PROPERTY_TIMER_DELIVER_MS}.
     * The time is aligned to {@code timerPrecisionMs} boundaries to match
     * the TimerWheel tick resolution.
     *
     * <p>The original topic and queue are saved as properties.
     * topic was changed to {@link TimerMessageStore#TIMER_TOPIC},
     * queue was changed to 0
     *
     * <p>Rejection conditions:
     * <ul>
     *   <li>Non-delay-level messages exceeding {@code timerMaxDelaySec}</li>
     *   <li>TimerWheel slot congestion ({@link TimerMessageStore#isReject})</li>
     * </ul>
     *
     * @param brokerController the broker controller
     * @param msg              the message to transform
     * @return a non-null {@link PutMessageResult} if the message is rejected
     */
    private static PutMessageResult transformTimerMessage(BrokerController brokerController,
        MessageExtBrokerInner msg) {
        //do transform
        int delayLevel = msg.getDelayTimeLevel();

        // calculate deliver time
        long deliverMs;
        try {
            if (msg.getProperty(MessageConst.PROPERTY_TIMER_DELAY_SEC) != null) {
                deliverMs = System.currentTimeMillis() + Long.parseLong(msg.getProperty(MessageConst.PROPERTY_TIMER_DELAY_SEC)) * 1000;
            } else if (msg.getProperty(MessageConst.PROPERTY_TIMER_DELAY_MS) != null) {
                deliverMs = System.currentTimeMillis() + Long.parseLong(msg.getProperty(MessageConst.PROPERTY_TIMER_DELAY_MS));
            } else {
                deliverMs = Long.parseLong(msg.getProperty(MessageConst.PROPERTY_TIMER_DELIVER_MS));
            }
        } catch (Exception e) {
            return new PutMessageResult(PutMessageStatus.WHEEL_TIMER_MSG_ILLEGAL, null);
        }

        if (deliverMs > System.currentTimeMillis()) {
            // default value of timerMaxDelaySec is 3600 * 24 * 3
            if (delayLevel <= 0 && deliverMs - System.currentTimeMillis() > brokerController.getMessageStoreConfig().getTimerMaxDelaySec() * 1000L) {
                return new PutMessageResult(PutMessageStatus.WHEEL_TIMER_MSG_ILLEGAL, null);
            }

            // precision operation
            int timerPrecisionMs = brokerController.getMessageStoreConfig().getTimerPrecisionMs();
            if (deliverMs % timerPrecisionMs == 0) {
                // Exactly on boundary → move one tick earlier
                deliverMs -= timerPrecisionMs;
            } else {
                // Not on boundary → round down to nearest tick
                deliverMs = deliverMs / timerPrecisionMs * timerPrecisionMs;
            }

            // flow control, always skip with default config
            if (brokerController.getTimerMessageStore().isReject(deliverMs)) {
                return new PutMessageResult(PutMessageStatus.WHEEL_TIMER_FLOW_CONTROL, null);
            }

            MessageAccessor.putProperty(msg, MessageConst.PROPERTY_TIMER_OUT_MS, deliverMs + "");
            MessageAccessor.putProperty(msg, MessageConst.PROPERTY_REAL_TOPIC, msg.getTopic());
            MessageAccessor.putProperty(msg, MessageConst.PROPERTY_REAL_QUEUE_ID, String.valueOf(msg.getQueueId()));
            msg.setPropertiesString(MessageDecoder.messageProperties2String(msg.getProperties()));
            msg.setTopic(TimerMessageStore.TIMER_TOPIC);
            msg.setQueueId(0);
        } else if (null != msg.getProperty(MessageConst.PROPERTY_TIMER_DEL_UNIQKEY)) {
            return new PutMessageResult(PutMessageStatus.WHEEL_TIMER_MSG_ILLEGAL, null);
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

    /**
     * Forward messages to another broker (typically the retry / dead-letter
     * queue destination). Used as the {@link SendMessageBackHook} implementation.
     *
     * <p>Each message is sent with {@code waitStoreMsgOK=false} and a 3s timeout.
     * Messages are removed from the list on success; on any failure the entire
     * batch is aborted and {@code false} is returned.
     */
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
