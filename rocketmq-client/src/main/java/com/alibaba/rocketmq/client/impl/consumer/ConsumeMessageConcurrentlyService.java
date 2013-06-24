package com.alibaba.rocketmq.client.impl.consumer;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;

import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import com.alibaba.rocketmq.client.log.ClientLogger;
import com.alibaba.rocketmq.common.MixAll;
import com.alibaba.rocketmq.common.message.Message;
import com.alibaba.rocketmq.common.message.MessageExt;
import com.alibaba.rocketmq.common.message.MessageQueue;


/**
 * 并发消费消息服务
 * 
 * @author shijia.wxr<vintage.wang@gmail.com>
 */
public class ConsumeMessageConcurrentlyService implements ConsumeMessageService {
    private static final Logger log = ClientLogger.getLog();
    private final DefaultMQPushConsumerImpl defaultMQPushConsumerImpl;
    private final DefaultMQPushConsumer defaultMQPushConsumer;
    private final MessageListenerConcurrently messageListener;
    private final BlockingQueue<Runnable> consumeRequestQueue;
    private final ExecutorService consumeExecutor;
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
            new ThreadFactory() {
                private AtomicLong threadIndex = new AtomicLong(0);


                @Override
                public Thread newThread(Runnable r) {
                    return new Thread(r, "ConsumeMessageThread-" //
                            + ConsumeMessageConcurrentlyService.this.consumerGroup//
                            + "-" + this.threadIndex.incrementAndGet());
                }
            });

        this.scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                return new Thread(r, "ConsumeMessageScheduledThread-" + consumerGroup);
            }
        });
    }


    public void start() {
    }


    public void shutdown() {
        this.consumeExecutor.shutdown();
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
                String retryTopic = msg.getProperty(Message.PROPERTY_RETRY_TOPIC);
                if (retryTopic != null && groupTopic.equals(msg.getTopic())) {
                    msg.setTopic(retryTopic);
                }
            }
        }


        @Override
        public void run() {
            MessageListenerConcurrently listener = ConsumeMessageConcurrentlyService.this.messageListener;
            ConsumeConcurrentlyContext context = new ConsumeConcurrentlyContext(messageQueue);
            ConsumeConcurrentlyStatus status = null;
            try {
                this.resetRetryTopic(msgs);
                status = listener.consumeMessage(msgs, context);
            }
            catch (Throwable e) {
                log.warn("consumeMessage exception, Group: "
                        + ConsumeMessageConcurrentlyService.this.consumerGroup//
                        + " " + msgs//
                        + " " + messageQueue, e);
            }

            if (null == status) {
                status = ConsumeConcurrentlyStatus.RECONSUME_LATER;
            }

            ConsumeMessageConcurrentlyService.this.processConsumeResult(status, context, this);
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
        // 客户端自动决定定时级别
        int delayLevel = 3;

        if (context.getDelayLevelWhenNextConsume() <= 0) {
            int reconsumeTimes = msg.getReconsumeTimes();
            if (reconsumeTimes > 0) {
                // 每个级别重试若干次
                delayLevel += delayLevel / 3;
            }
        }
        else {
            delayLevel = context.getDelayLevelWhenNextConsume();
        }

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
            break;
        case RECONSUME_LATER:
            ackIndex = -1;
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
        if (offset > 0) {
            this.defaultMQPushConsumerImpl.updateConsumeOffset(consumeRequest.getMessageQueue(), offset);
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
                ConsumeMessageConcurrentlyService.this.submitConsumeRequest(msgs, processQueue, messageQueue);
            }
        }, 5000, TimeUnit.MILLISECONDS);
    }


    @Override
    public void submitConsumeRequest(List<MessageExt> msgs, ProcessQueue processQueue,
            MessageQueue messageQueue) {
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
}
