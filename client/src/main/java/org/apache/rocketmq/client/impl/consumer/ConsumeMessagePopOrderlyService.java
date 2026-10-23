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
package org.apache.rocketmq.client.impl.consumer;

import io.netty.util.internal.ConcurrentSet;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.client.consumer.AckCallback;
import org.apache.rocketmq.client.consumer.AckResult;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyStatus;
import org.apache.rocketmq.client.consumer.listener.ConsumeReturnType;
import org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly;
import org.apache.rocketmq.client.hook.ConsumeMessageContext;
import org.apache.rocketmq.client.stat.ConsumerStatsManager;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.protocol.NamespaceUtil;
import org.apache.rocketmq.remoting.protocol.body.CMResult;
import org.apache.rocketmq.remoting.protocol.body.ConsumeMessageDirectlyResult;
import org.apache.rocketmq.remoting.protocol.header.ExtraInfoUtil;
import org.apache.rocketmq.remoting.protocol.heartbeat.MessageModel;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

public class ConsumeMessagePopOrderlyService extends AbstractConsumeMessageService {
    private static final Logger log = LoggerFactory.getLogger(ConsumeMessagePopOrderlyService.class);
    private final DefaultMQPushConsumerImpl defaultMQPushConsumerImpl;
    private final MessageListenerOrderly messageListener;
    private final ConcurrentSet<ConsumeRequest> consumeRequestSet = new ConcurrentSet<>();
    private final String consumerGroup;
    private final MessageQueueLock messageQueueLock = new MessageQueueLock();
    private final MessageQueueLock consumeRequestLock = new MessageQueueLock();
    private final ScheduledExecutorService scheduledExecutorService;
    private volatile boolean stopped = false;

    public ConsumeMessagePopOrderlyService(DefaultMQPushConsumerImpl defaultMQPushConsumerImpl,
        MessageListenerOrderly messageListener) {
        super(defaultMQPushConsumerImpl.getDefaultMQPushConsumer(), new ThreadFactoryImpl("ConsumeMessageThread_"));
        this.defaultMQPushConsumerImpl = defaultMQPushConsumerImpl;
        this.messageListener = messageListener;

        this.consumerGroup = this.defaultMQPushConsumer.getConsumerGroup();

        this.scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryImpl("ConsumeMessageScheduledThread_"));
    }

    @Override
    public void start() {
        if (MessageModel.CLUSTERING.equals(ConsumeMessagePopOrderlyService.this.defaultMQPushConsumerImpl.messageModel())) {
            this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
                @Override
                public void run() {
                    ConsumeMessagePopOrderlyService.this.lockMQPeriodically();
                }
            }, 1000 * 1, ProcessQueue.REBALANCE_LOCK_INTERVAL, TimeUnit.MILLISECONDS);
        }
    }

    @Override
    public void shutdown(long awaitTerminateMillis) {
        this.stopped = true;
        this.scheduledExecutorService.shutdown();
        shutdownConsumeExecutor(awaitTerminateMillis);
        if (MessageModel.CLUSTERING.equals(this.defaultMQPushConsumerImpl.messageModel())) {
            this.unlockAllMessageQueues();
        }
    }

    public synchronized void unlockAllMessageQueues() {
        this.defaultMQPushConsumerImpl.getRebalanceImpl().unlockAll(false);
    }

    @Override
    public ConsumeMessageDirectlyResult consumeMessageDirectly(MessageExt msg, String brokerName) {
        ConsumeMessageDirectlyResult result = new ConsumeMessageDirectlyResult();
        result.setOrder(true);

        List<MessageExt> msgs = new ArrayList<>();
        msgs.add(msg);
        MessageQueue mq = new MessageQueue();
        mq.setBrokerName(brokerName);
        mq.setTopic(msg.getTopic());
        mq.setQueueId(msg.getQueueId());

        ConsumeOrderlyContext context = new ConsumeOrderlyContext(mq);

        this.defaultMQPushConsumerImpl.resetRetryAndNamespace(msgs, this.consumerGroup);

        final long beginTime = System.currentTimeMillis();

        log.info("consumeMessageDirectly receive new message: {}", msg);

        try {
            ConsumeOrderlyStatus status = this.messageListener.consumeMessage(msgs, context);
            if (status != null) {
                switch (status) {
                    case COMMIT:
                        result.setConsumeResult(CMResult.CR_COMMIT);
                        break;
                    case ROLLBACK:
                        result.setConsumeResult(CMResult.CR_ROLLBACK);
                        break;
                    case SUCCESS:
                        result.setConsumeResult(CMResult.CR_SUCCESS);
                        break;
                    case SUSPEND_CURRENT_QUEUE_A_MOMENT:
                        result.setConsumeResult(CMResult.CR_LATER);
                        break;
                    default:
                        break;
                }
            } else {
                result.setConsumeResult(CMResult.CR_RETURN_NULL);
            }
        } catch (Throwable e) {
            result.setConsumeResult(CMResult.CR_THROW_EXCEPTION);
            result.setRemark(UtilAll.exceptionSimpleDesc(e));

            log.warn("consumeMessageDirectly exception: {} Group: {} Msgs: {} MQ: {}",
                UtilAll.exceptionSimpleDesc(e),
                ConsumeMessagePopOrderlyService.this.consumerGroup,
                msgs,
                mq, e);
        }

        result.setAutoCommit(context.isAutoCommit());
        result.setSpentTimeMills(System.currentTimeMillis() - beginTime);

        log.info("consumeMessageDirectly Result: {}", result);

        return result;
    }

    @Override
    public void submitConsumeRequest(List<MessageExt> msgs, ProcessQueue processQueue,
                                     MessageQueue messageQueue, boolean dispathToConsume) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void submitPopConsumeRequest(final List<MessageExt> msgs,
                                     final PopProcessQueue processQueue,
                                     final MessageQueue messageQueue) {
        if (msgs == null || msgs.isEmpty()) {
            return;
        }
        ConsumeRequest req = new ConsumeRequest(msgs, processQueue, messageQueue);
        submitConsumeRequest(req);
    }

    public synchronized void lockMQPeriodically() {
        if (!this.stopped) {
            this.defaultMQPushConsumerImpl.getRebalanceImpl().lockAll();
        }
    }

    private ConsumeRequest findConsumeRequest(final ConsumeRequest consumeRequest) {
        for (ConsumeRequest request : consumeRequestSet) {
            if (request.equals(consumeRequest)) {
                return request;
            }
        }
        return null;
    }

    private void submitConsumeRequest(final ConsumeRequest consumeRequest) {
        Object lock = consumeRequestLock.fetchLockObject(consumeRequest.getMessageQueue(), consumeRequest.shardingKeyIndex);
        synchronized (lock) {
            ConsumeRequest existing = findConsumeRequest(consumeRequest);
            if (existing != null) {
                // A previous pop result for this queue/shard is still being consumed. Keep the
                // new messages in that request instead of dropping them: the request drains its
                // pending batches under the same lock before it removes itself.
                if (existing != consumeRequest) {
                    existing.mergeFrom(consumeRequest);
                }
                return;
            }

            consumeRequestSet.add(consumeRequest);
            try {
                consumeExecutor.submit(consumeRequest);
            } catch (Exception e) {
                log.error("error submit consume request: {}, mq: {}, shardingKeyIndex: {}",
                    e.toString(), consumeRequest.getMessageQueue(), consumeRequest.getShardingKeyIndex());
                // Do not leave the request behind: while it stays in consumeRequestSet every later
                // pop result for this queue is merged into a request that no executor will run.
                consumeRequestSet.remove(consumeRequest);
                submitConsumeRequestLater(consumeRequest);
            }
        }
    }

    private void submitConsumeRequestLater(final ConsumeRequest consumeRequest) {
        try {
            this.scheduledExecutorService.schedule(new Runnable() {
                @Override
                public void run() {
                    submitConsumeRequest(consumeRequest);
                }
            }, 5000, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            log.warn("the consume scheduled executor has been shut down, discard the pop request for {}",
                consumeRequest.getMessageQueue());
            consumeRequest.releaseWaitAckForPendingBatches();
        }
    }

    public boolean processConsumeResult(
        final List<MessageExt> msgs,
        final ConsumeOrderlyStatus status,
        final ConsumeOrderlyContext context,
        final ConsumeRequest consumeRequest
    ) {
        if (msgs == null || msgs.isEmpty()) {
            return true;
        }

        String topic = consumeRequest.getMessageQueue().getTopic();

        // POP mode has no local offset to advance. COMMIT and SUCCESS both mean that the broker
        // can forget the popped copy, while ROLLBACK and SUSPEND keep it available for redelivery.
        if (ConsumeOrderlyStatus.SUCCESS == status || ConsumeOrderlyStatus.COMMIT == status) {
            this.getConsumerStatsManager().incConsumeOKTPS(consumerGroup, topic, msgs.size());
            for (MessageExt msg : msgs) {
                ackMessage(msg, consumeRequest);
            }
            return true;
        }

        this.getConsumerStatsManager().incConsumeFailedTPS(consumerGroup, topic, msgs.size());

        List<MessageExt> sentBack = new ArrayList<>();
        boolean needRetry = checkReconsumeTimes(msgs, sentBack);
        for (MessageExt msg : msgs) {
            // The message is no longer waiting for a local ack even when it has to be retried.
            // For retries the broker owns it again after the invisible-time change.
            consumeRequest.getProcessQueue().ack();
            if (sentBack.contains(msg)) {
                this.defaultMQPushConsumerImpl.ackAsync(msg, consumerGroup);
            } else {
                changePopInvisibleTime(msg, context);
            }
        }

        // A failed batch must block later batches of the same ordered queue. Those messages are
        // already invisible on the broker and will be redelivered after their own deadline.
        return !needRetry;
    }

    private void ackMessage(final MessageExt msg, final ConsumeRequest consumeRequest) {
        this.defaultMQPushConsumerImpl.ackAsync(msg, consumerGroup);
        consumeRequest.getProcessQueue().ack();
    }

    public ConsumerStatsManager getConsumerStatsManager() {
        return this.defaultMQPushConsumerImpl.getConsumerStatsManager();
    }

    private int getMaxReconsumeTimes() {
        // default reconsume times: Integer.MAX_VALUE
        if (this.defaultMQPushConsumer.getMaxReconsumeTimes() == -1) {
            return Integer.MAX_VALUE;
        } else {
            return this.defaultMQPushConsumer.getMaxReconsumeTimes();
        }
    }

    private boolean checkReconsumeTimes(List<MessageExt> msgs, List<MessageExt> sentBack) {
        boolean suspend = false;
        if (msgs != null && !msgs.isEmpty()) {
            for (MessageExt msg : msgs) {
                if (msg.getReconsumeTimes() >= getMaxReconsumeTimes()) {
                    MessageAccessor.setReconsumeTime(msg, String.valueOf(msg.getReconsumeTimes()));
                    if (sendMessageBack(msg)) {
                        sentBack.add(msg);
                    } else {
                        suspend = true;
                        msg.setReconsumeTimes(msg.getReconsumeTimes() + 1);
                    }
                } else {
                    suspend = true;
                    msg.setReconsumeTimes(msg.getReconsumeTimes() + 1);
                }
            }
        }
        return suspend;
    }

    private void changePopInvisibleTime(final MessageExt msg, final ConsumeOrderlyContext context) {
        long invisibleTime = context.getSuspendCurrentQueueTimeMillis();
        if (invisibleTime <= 0) {
            int[] delayLevelTable = this.defaultMQPushConsumerImpl.getPopDelayLevel();
            if (delayLevelTable == null || delayLevelTable.length == 0) {
                invisibleTime = 60000L;
            } else {
                int delayLevel = Math.min(msg.getReconsumeTimes(), delayLevelTable.length - 1);
                invisibleTime = delayLevelTable[delayLevel] * 1000L;
            }
        }

        String extraInfo = msg.getProperty(MessageConst.PROPERTY_POP_CK);
        if (StringUtils.isBlank(extraInfo)) {
            log.warn("can not change invisible time without pop check point, group: {}, msg: {}",
                consumerGroup, msg);
            return;
        }

        try {
            this.defaultMQPushConsumerImpl.changePopInvisibleTimeAsync(msg.getTopic(), consumerGroup, extraInfo,
                invisibleTime, new AckCallback() {
                    @Override
                    public void onSuccess(AckResult ackResult) {
                    }

                    @Override
                    public void onException(Throwable e) {
                        log.error("changePopInvisibleTimeAsync fail. msg:{} error info: {}", msg, e.toString());
                    }
                });
        } catch (Throwable t) {
            log.error("changePopInvisibleTimeAsync fail, group:{} msg:{} errorInfo:{}", consumerGroup, msg, t.toString());
        }
    }

    public boolean sendMessageBack(final MessageExt msg) {
        try {
            // max reconsume times exceeded then send to dead letter queue.
            Message newMsg = new Message(MixAll.getRetryTopic(this.defaultMQPushConsumer.getConsumerGroup()), msg.getBody());
            MessageAccessor.setProperties(newMsg, msg.getProperties());
            String originMsgId = MessageAccessor.getOriginMessageId(msg);
            MessageAccessor.setOriginMessageId(newMsg, UtilAll.isBlank(originMsgId) ? msg.getMsgId() : originMsgId);
            newMsg.setFlag(msg.getFlag());
            MessageAccessor.putProperty(newMsg, MessageConst.PROPERTY_RETRY_TOPIC, msg.getTopic());
            MessageAccessor.setReconsumeTime(newMsg, String.valueOf(msg.getReconsumeTimes()));
            MessageAccessor.setMaxReconsumeTimes(newMsg, String.valueOf(getMaxReconsumeTimes()));
            newMsg.setDelayTimeLevel(3 + msg.getReconsumeTimes());

            this.defaultMQPushConsumerImpl.getmQClientFactory().getDefaultMQProducer().send(newMsg);
            return true;
        } catch (Exception e) {
            log.error("sendMessageBack exception, group: " + this.consumerGroup + " msg: " + msg.toString(), e);
        }

        return false;
    }

    public void resetNamespace(final List<MessageExt> msgs) {
        for (MessageExt msg : msgs) {
            if (StringUtils.isNotEmpty(this.defaultMQPushConsumer.getNamespace())) {
                msg.setTopic(NamespaceUtil.withoutNamespace(msg.getTopic(), this.defaultMQPushConsumer.getNamespace()));
            }
        }
    }

    private static final class PopConsumeBatch {
        private final List<MessageExt> msgs;
        private final long popTime;
        private final long invisibleTime;

        private PopConsumeBatch(List<MessageExt> msgs) {
            this.msgs = msgs;
            long parsedPopTime = 0;
            long parsedInvisibleTime = 0;
            try {
                String extraInfo = msgs.get(0).getProperty(MessageConst.PROPERTY_POP_CK);
                String[] extraInfoStrs = ExtraInfoUtil.split(extraInfo);
                parsedPopTime = ExtraInfoUtil.getPopTime(extraInfoStrs);
                parsedInvisibleTime = ExtraInfoUtil.getInvisibleTime(extraInfoStrs);
            } catch (Throwable t) {
                log.error("parse extra info error. msg:" + msgs.get(0), t);
            }
            this.popTime = parsedPopTime;
            this.invisibleTime = parsedInvisibleTime;
        }

        private List<MessageExt> getMsgs() {
            return msgs;
        }

        private long getPopTime() {
            return popTime;
        }

        private long getInvisibleTime() {
            return invisibleTime;
        }

        private boolean isPopTimeout() {
            if (msgs.isEmpty() || popTime <= 0 || invisibleTime <= 0) {
                return true;
            }
            return System.currentTimeMillis() - popTime >= invisibleTime;
        }
    }

    class ConsumeRequest implements Runnable {
        private final PopProcessQueue processQueue;
        private final MessageQueue messageQueue;
        private final int shardingKeyIndex;
        private final Queue<PopConsumeBatch> pendingBatches = new ConcurrentLinkedQueue<>();

        public ConsumeRequest(List<MessageExt> msgs, PopProcessQueue processQueue, MessageQueue messageQueue) {
            this.processQueue = processQueue;
            this.messageQueue = messageQueue;
            // POP orderly consumes one ordered lane per message queue. The sharding key is kept so
            // that the existing lock helper has a stable key, but it always stays zero.
            this.shardingKeyIndex = 0;
            if (msgs != null && !msgs.isEmpty()) {
                this.pendingBatches.add(new PopConsumeBatch(msgs));
            }
        }

        public PopProcessQueue getProcessQueue() {
            return processQueue;
        }

        public MessageQueue getMessageQueue() {
            return messageQueue;
        }

        public int getShardingKeyIndex() {
            return shardingKeyIndex;
        }

        private void mergeFrom(final ConsumeRequest other) {
            PopConsumeBatch batch;
            while ((batch = other.pendingBatches.poll()) != null) {
                this.pendingBatches.add(batch);
            }
        }

        private void releaseWaitAck(int count) {
            if (count > 0) {
                this.processQueue.decFoundMsg(-count);
            }
        }

        private void releaseWaitAckForPendingBatches() {
            PopConsumeBatch batch;
            while ((batch = this.pendingBatches.poll()) != null) {
                releaseWaitAck(batch.getMsgs().size());
            }
        }

        /**
         * Remove this exact request from the shared set. ConsumeRequest equality is based on the
         * queue/shard, so a plain remove could delete a successor that was submitted after this
         * request had already been unregistered. Callers must hold the consume request lock.
         */
        private void removeFromConsumeRequestSet() {
            for (ConsumeRequest request : consumeRequestSet) {
                if (request == this) {
                    consumeRequestSet.remove(request);
                    return;
                }
            }
        }

        private void abortRemainingBatches() {
            Object lock = consumeRequestLock.fetchLockObject(this.messageQueue, this.shardingKeyIndex);
            synchronized (lock) {
                removeFromConsumeRequestSet();
                releaseWaitAckForPendingBatches();
            }
        }

        @Override
        public void run() {
            try {
                for (;;) {
                    PopConsumeBatch batch = this.pendingBatches.poll();
                    if (batch == null) {
                        // Check for work that was merged in while the last batch was being consumed,
                        // then remove the request atomically under the submission lock. Without this
                        // lock a concurrently submitted request could be appended after the empty
                        // check but before removal and would never be consumed.
                        Object lock = consumeRequestLock.fetchLockObject(this.messageQueue, this.shardingKeyIndex);
                        synchronized (lock) {
                            batch = this.pendingBatches.poll();
                            if (batch == null) {
                                removeFromConsumeRequestSet();
                                return;
                            }
                        }
                    }

                    if (!consumeBatch(batch)) {
                        abortRemainingBatches();
                        return;
                    }
                }
            } finally {
                abortRemainingBatches();
            }
        }

        private boolean consumeBatch(final PopConsumeBatch batch) {
            final List<MessageExt> msgs = batch.getMsgs();
            if (this.processQueue.isDropped()) {
                log.warn("run, message queue not be able to consume, because it's dropped. {}", this.messageQueue);
                releaseWaitAck(msgs.size());
                return false;
            }

            if (batch.isPopTimeout()) {
                log.info("the pop message time out so abort consume. popTime={} invisibleTime={}, group={} {}",
                    batch.getPopTime(), batch.getInvisibleTime(), consumerGroup, this.messageQueue);
                releaseWaitAck(msgs.size());
                return false;
            }

            // Ordered consumption must serialize all batches of the same queue/shard. The sharding
            // key index stays 0 because POP consumes the queue as a single ordered lane.
            final Object objLock = messageQueueLock.fetchLockObject(this.messageQueue, this.shardingKeyIndex);
            synchronized (objLock) {
                if (this.processQueue.isDropped()) {
                    log.warn("message queue not be able to consume, because it's dropped. {}", this.messageQueue);
                    releaseWaitAck(msgs.size());
                    return false;
                }

                if (batch.isPopTimeout()) {
                    log.info("the pop message time out before consume. popTime={} invisibleTime={}, group={} {}",
                        batch.getPopTime(), batch.getInvisibleTime(), consumerGroup, this.messageQueue);
                    releaseWaitAck(msgs.size());
                    return false;
                }

                defaultMQPushConsumerImpl.resetRetryAndNamespace(msgs, consumerGroup);

                ConsumeOrderlyContext context = new ConsumeOrderlyContext(this.messageQueue);
                ConsumeMessageContext consumeMessageContext = null;
                if (defaultMQPushConsumerImpl.hasHook()) {
                    consumeMessageContext = new ConsumeMessageContext();
                    consumeMessageContext.setNamespace(defaultMQPushConsumer.getNamespace());
                    consumeMessageContext.setConsumerGroup(defaultMQPushConsumer.getConsumerGroup());
                    consumeMessageContext.setProps(new HashMap<>());
                    consumeMessageContext.setMq(this.messageQueue);
                    consumeMessageContext.setMsgList(msgs);
                    consumeMessageContext.setSuccess(false);
                    defaultMQPushConsumerImpl.executeHookBefore(consumeMessageContext);
                }

                long beginTimestamp = System.currentTimeMillis();
                boolean hasException = false;
                ConsumeReturnType returnType = ConsumeReturnType.SUCCESS;
                ConsumeOrderlyStatus status = null;
                try {
                    for (MessageExt msg : msgs) {
                        MessageAccessor.setConsumeStartTimeStamp(msg, String.valueOf(System.currentTimeMillis()));
                    }
                    status = messageListener.consumeMessage(Collections.unmodifiableList(msgs), context);
                } catch (Throwable e) {
                    log.warn("consumeMessage exception: {} Group: {} Msgs: {} MQ: {}",
                        UtilAll.exceptionSimpleDesc(e), consumerGroup, msgs, this.messageQueue, e);
                    hasException = true;
                }

                long consumeRT = System.currentTimeMillis() - beginTimestamp;
                if (null == status) {
                    if (hasException) {
                        returnType = ConsumeReturnType.EXCEPTION;
                    } else {
                        returnType = ConsumeReturnType.RETURNNULL;
                    }
                    log.warn("consumeMessage return null, Group: {} Msgs: {} MQ: {}",
                        consumerGroup, msgs, this.messageQueue);
                    status = ConsumeOrderlyStatus.SUSPEND_CURRENT_QUEUE_A_MOMENT;
                } else if (consumeRT >= defaultMQPushConsumer.getConsumeTimeout() * 60 * 1000) {
                    returnType = ConsumeReturnType.TIME_OUT;
                } else if (ConsumeOrderlyStatus.SUSPEND_CURRENT_QUEUE_A_MOMENT == status) {
                    returnType = ConsumeReturnType.FAILED;
                } else if (ConsumeOrderlyStatus.SUCCESS == status) {
                    returnType = ConsumeReturnType.SUCCESS;
                }

                if (defaultMQPushConsumerImpl.hasHook()) {
                    consumeMessageContext.getProps().put(MixAll.CONSUME_CONTEXT_TYPE, returnType.name());
                    consumeMessageContext.setStatus(status.toString());
                    consumeMessageContext.setSuccess(ConsumeOrderlyStatus.SUCCESS == status
                        || ConsumeOrderlyStatus.COMMIT == status);
                    consumeMessageContext.setAccessChannel(defaultMQPushConsumer.getAccessChannel());
                    defaultMQPushConsumerImpl.executeHookAfter(consumeMessageContext);
                }

                getConsumerStatsManager().incConsumeRT(consumerGroup, this.messageQueue.getTopic(), consumeRT);

                if (this.processQueue.isDropped() || batch.isPopTimeout()) {
                    log.warn("processQueue invalid or popTimeout after consume. isDropped={}, isPopTimeout={}, messageQueue={}, msgs={}",
                        this.processQueue.isDropped(), batch.isPopTimeout(), this.messageQueue, msgs);
                    releaseWaitAck(msgs.size());
                    return false;
                }

                return processConsumeResult(msgs, status, context, this);
            }
        }

        @Override
        public int hashCode() {
            int hash = shardingKeyIndex;
            if (processQueue != null) {
                hash += processQueue.hashCode() * 31;
            }
            if (messageQueue != null) {
                hash += messageQueue.hashCode() * 31;
            }
            return hash;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }

            ConsumeRequest other = (ConsumeRequest) obj;
            if (shardingKeyIndex != other.shardingKeyIndex) {
                return false;
            }

            if (processQueue != other.processQueue) {
                return false;
            }

            if (messageQueue == other.messageQueue) {
                return true;
            }
            if (messageQueue != null && messageQueue.equals(other.messageQueue)) {
                return true;
            }
            return false;
        }
    }
}
