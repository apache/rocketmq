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
package com.alibaba.rocketmq.client.impl.consumer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;

import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import com.alibaba.rocketmq.client.hook.ConsumeMessageContext;
import com.alibaba.rocketmq.client.log.ClientLogger;
import com.alibaba.rocketmq.client.stat.ConsumerStat;
import com.alibaba.rocketmq.common.MixAll;
import com.alibaba.rocketmq.common.ThreadFactoryImpl;
import com.alibaba.rocketmq.common.message.MessageConst;
import com.alibaba.rocketmq.common.message.MessageExt;
import com.alibaba.rocketmq.common.message.MessageQueue;
import com.alibaba.rocketmq.remoting.common.RemotingHelper;


/**
 * 并发消费消息服务
 * 
 * @author shijia.wxr<vintage.wang@gmail.com>
 * @since 2013-7-24
 */
public class ConsumeMessageConcurrentlyService implements ConsumeMessageService {
    private static final Logger log = ClientLogger.getLog();
    private final DefaultMQPushConsumerImpl defaultMQPushConsumerImpl;
    private final DefaultMQPushConsumer defaultMQPushConsumer;
    private final MessageListenerConcurrently messageListener;
    private final BlockingQueue<Runnable> consumeRequestQueue;
    private final ThreadPoolExecutor consumeExecutor;
    private final String consumerGroup;

    // 定时线程
    private final ScheduledExecutorService scheduledExecutorService;


    public ConsumeMessageConcurrentlyService(DefaultMQPushConsumerImpl defaultMQPushConsumerImpl,
            MessageListenerConcurrently messageListener) {
        this.defaultMQPushConsumerImpl = defaultMQPushConsumerImpl;
        this.messageListener = messageListener;

        this.defaultMQPushConsumer = this.defaultMQPushConsumerImpl.getDefaultMQPushConsumer();
        this.consumerGroup = this.defaultMQPushConsumer.getConsumerGroup();
        this.consumeRequestQueue = new LinkedBlockingQueue<Runnable>();

        this.consumeExecutor = new ThreadPoolExecutor(//
            this.defaultMQPushConsumer.getConsumeThreadMin(),//
            this.defaultMQPushConsumer.getConsumeThreadMax(),//
            1000 * 60,//
            TimeUnit.MILLISECONDS,//
            this.consumeRequestQueue,//
            new ThreadFactoryImpl("ConsumeMessageThread_"));

        this.scheduledExecutorService =
                Executors.newSingleThreadScheduledExecutor(new ThreadFactoryImpl(
                    "ConsumeMessageScheduledThread_"));
    }


    public void start() {
    }


    public void shutdown() {
        this.scheduledExecutorService.shutdown();
        this.consumeExecutor.shutdown();
    }


    public ConsumerStat getConsumerStat() {
        return this.defaultMQPushConsumerImpl.getConsumerStatManager().getConsumertat();
    }

    class ConsumeRequest implements Runnable {
        private final List<MessageExt> msgs;
        private final ProcessQueue processQueue;
        private final MessageQueue messageQueue;


        public ConsumeRequest(List<MessageExt> msgs, ProcessQueue processQueue, MessageQueue messageQueue) {
            this.msgs = msgs;
            this.processQueue = processQueue;
            this.messageQueue = messageQueue;
        }


        private void resetRetryTopic(final List<MessageExt> msgs) {
            final String groupTopic = MixAll.getRetryTopic(consumerGroup);
            for (MessageExt msg : msgs) {
                String retryTopic = msg.getProperty(MessageConst.PROPERTY_RETRY_TOPIC);
                if (retryTopic != null && groupTopic.equals(msg.getTopic())) {
                    msg.setTopic(retryTopic);
                }
            }
        }


        @Override
        public void run() {
            if (this.processQueue.isDroped()) {
                log.info("the message queue not be able to consume, because it's dropped {}",
                    this.messageQueue);
                return;
            }

            MessageListenerConcurrently listener = ConsumeMessageConcurrentlyService.this.messageListener;
            ConsumeConcurrentlyContext context = new ConsumeConcurrentlyContext(messageQueue);
            ConsumeConcurrentlyStatus status = null;

            // 执行Hook
            ConsumeMessageContext consumeMessageContext = null;
            if (ConsumeMessageConcurrentlyService.this.defaultMQPushConsumerImpl.hasHook()) {
                consumeMessageContext = new ConsumeMessageContext();
                consumeMessageContext
                    .setConsumerGroup(ConsumeMessageConcurrentlyService.this.defaultMQPushConsumer
                        .getConsumerGroup());
                consumeMessageContext.setMq(messageQueue);
                consumeMessageContext.setMsgList(msgs);
                consumeMessageContext.setSuccess(false);
                ConsumeMessageConcurrentlyService.this.defaultMQPushConsumerImpl
                    .executeHookBefore(consumeMessageContext);
            }

            long beginTimestamp = System.currentTimeMillis();

            try {
                this.resetRetryTopic(msgs);
                status = listener.consumeMessage(Collections.unmodifiableList(msgs), context);
            }
            catch (Throwable e) {
                log.warn("consumeMessage exception: {} Group: {} Msgs: {} MQ: {}",//
                    RemotingHelper.exceptionSimpleDesc(e),//
                    ConsumeMessageConcurrentlyService.this.consumerGroup,//
                    msgs,//
                    messageQueue);
            }

            long consumeRT = System.currentTimeMillis() - beginTimestamp;

            if (null == status) {
                log.warn("consumeMessage return null, Group: {} Msgs: {} MQ: {}",//
                    ConsumeMessageConcurrentlyService.this.consumerGroup,//
                    msgs,//
                    messageQueue);
                status = ConsumeConcurrentlyStatus.RECONSUME_LATER;
            }

            // 执行Hook
            if (ConsumeMessageConcurrentlyService.this.defaultMQPushConsumerImpl.hasHook()) {
                consumeMessageContext.setSuccess(ConsumeConcurrentlyStatus.CONSUME_SUCCESS == status);
                ConsumeMessageConcurrentlyService.this.defaultMQPushConsumerImpl
                    .executeHookAfter(consumeMessageContext);
            }

            // 记录统计信息
            ConsumeMessageConcurrentlyService.this.getConsumerStat().getConsumeMsgRTTotal()
                .addAndGet(consumeRT);
            boolean updated =
                    MixAll.compareAndIncreaseOnly(ConsumeMessageConcurrentlyService.this.getConsumerStat()
                        .getConsumeMsgRTMax(), consumeRT);
            // 耗时最大值新记录
            if (updated) {
                log.warn("consumeMessage RT new max: {} Group: {} Msgs: {} MQ: {}",//
                    consumeRT,//
                    ConsumeMessageConcurrentlyService.this.consumerGroup,//
                    msgs,//
                    messageQueue);
            }

            // 如果ProcessQueue是dropped状态，不需要直接更新 offset
            if (!processQueue.isDroped()) {
                ConsumeMessageConcurrentlyService.this.processConsumeResult(status, context, this);
            }
            else {
                log.warn(
                    "processQueue is dropped without process consume result. messageQueue={}, msgTreeMap={}, msgs={}",
                    new Object[] { messageQueue, processQueue.getMsgTreeMap(), msgs });
            }
        }


        public List<MessageExt> getMsgs() {
            return msgs;
        }


        public ProcessQueue getProcessQueue() {
            return processQueue;
        }


        public MessageQueue getMessageQueue() {
            return messageQueue;
        }
    }


    public boolean sendMessageBack(final MessageExt msg, final ConsumeConcurrentlyContext context) {
        // 如果用户没有设置，服务器会根据重试次数自动叠加延时时间
        int delayLevel = context.getDelayLevelWhenNextConsume();

        try {
            this.defaultMQPushConsumerImpl.sendMessageBack(msg, delayLevel);
            return true;
        }
        catch (Exception e) {
            log.error("sendMessageBack exception, group: " + this.consumerGroup + " msg: " + msg.toString(),
                e);
        }

        return false;
    }


    public void processConsumeResult(//
            final ConsumeConcurrentlyStatus status, //
            final ConsumeConcurrentlyContext context, //
            final ConsumeRequest consumeRequest//
    ) {
        int ackIndex = context.getAckIndex();

        if (consumeRequest.getMsgs().isEmpty())
            return;

        switch (status) {
        case CONSUME_SUCCESS:
            if (ackIndex >= consumeRequest.getMsgs().size()) {
                ackIndex = consumeRequest.getMsgs().size() - 1;
            }
            int ok = ackIndex + 1;
            int failed = consumeRequest.getMsgs().size() - ok;
            // 统计信息
            this.getConsumerStat().getConsumeMsgOKTotal().addAndGet(ok);
            this.getConsumerStat().getConsumeMsgFailedTotal().addAndGet(failed);
            break;
        case RECONSUME_LATER:
            ackIndex = -1;
            // 统计信息
            this.getConsumerStat().getConsumeMsgFailedTotal().addAndGet(consumeRequest.getMsgs().size());
            break;
        default:
            break;
        }

        switch (this.defaultMQPushConsumer.getMessageModel()) {
        case BROADCASTING:
            // 如果是广播模式，直接丢弃失败消息，需要在文档中告知用户
            // 这样做的原因：广播模式对于失败重试代价过高，对整个集群性能会有较大影响，失败重试功能交由应用处理
            for (int i = ackIndex + 1; i < consumeRequest.getMsgs().size(); i++) {
                MessageExt msg = consumeRequest.getMsgs().get(i);
                log.warn("BROADCASTING, the message consume failed, drop it, {}", msg.toString());
            }
            break;
        case CLUSTERING:
            // 处理消费失败的消息，直接发回到Broker
            List<MessageExt> msgBackFailed = new ArrayList<MessageExt>(consumeRequest.getMsgs().size());
            for (int i = ackIndex + 1; i < consumeRequest.getMsgs().size(); i++) {
                MessageExt msg = consumeRequest.getMsgs().get(i);
                boolean result = this.sendMessageBack(msg, context);
                if (!result) {
                    msg.setReconsumeTimes(msg.getReconsumeTimes() + 1);
                    msgBackFailed.add(msg);
                }
            }

            if (!msgBackFailed.isEmpty()) {
                // 发回失败的消息仍然要保留
                consumeRequest.getMsgs().removeAll(msgBackFailed);

                // 此过程处理失败的消息，需要在Client中做定时消费，直到成功
                this.submitConsumeRequestLater(msgBackFailed, consumeRequest.getProcessQueue(),
                    consumeRequest.getMessageQueue());
            }
            break;
        default:
            break;
        }

        long offset = consumeRequest.getProcessQueue().removeMessage(consumeRequest.getMsgs());
        if (offset >= 0) {
            this.defaultMQPushConsumerImpl.getOffsetStore().updateOffset(consumeRequest.getMessageQueue(),
                offset, true);
        }
    }


    /**
     * 在Consumer本地定时线程中定时重试
     */
    private void submitConsumeRequestLater(//
            final List<MessageExt> msgs, //
            final ProcessQueue processQueue, //
            final MessageQueue messageQueue//
    ) {

        this.scheduledExecutorService.schedule(new Runnable() {

            @Override
            public void run() {
                ConsumeMessageConcurrentlyService.this.submitConsumeRequest(msgs, processQueue, messageQueue,
                    true);
            }
        }, 5000, TimeUnit.MILLISECONDS);
    }


    @Override
    public void submitConsumeRequest(//
            final List<MessageExt> msgs, //
            final ProcessQueue processQueue, //
            final MessageQueue messageQueue, //
            final boolean dispathToConsume) {
        final int consumeBatchSize = this.defaultMQPushConsumer.getConsumeMessageBatchMaxSize();
        if (msgs.size() <= consumeBatchSize) {
            ConsumeRequest consumeRequest = new ConsumeRequest(msgs, processQueue, messageQueue);
            this.consumeExecutor.submit(consumeRequest);
        }
        else {
            for (int total = 0; total < msgs.size();) {
                List<MessageExt> msgThis = new ArrayList<MessageExt>(consumeBatchSize);
                for (int i = 0; i < consumeBatchSize; i++, total++) {
                    if (total < msgs.size()) {
                        msgThis.add(msgs.get(total));
                    }
                    else {
                        break;
                    }
                }

                ConsumeRequest consumeRequest = new ConsumeRequest(msgThis, processQueue, messageQueue);
                this.consumeExecutor.submit(consumeRequest);
            }
        }
    }


    @Override
    public void updateCorePoolSize(int corePoolSize) {
        if (corePoolSize > 0 && corePoolSize <= Short.MAX_VALUE) {
            this.consumeExecutor.setCorePoolSize(corePoolSize);
        }
    }
}
