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
package com.alibaba.rocketmq.client.consumer;

import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.common.message.MessageQueue;
import com.alibaba.rocketmq.common.protocol.heartbeat.MessageModel;


/**
 * PullConsumer的调度服务，降低Pull方式的编程复杂度
 * 
 * @author shijia.wxr<vintage.wang@gmail.com>
 * @since 2014-2-26
 */
public class MQPullConsumerScheduleService {
    private DefaultMQPullConsumer defaultMQPullConsumer;
    private int pullThreadNums = 20;

    /**
     * 具体实现，使用者不用关心
     */
    private ThreadPoolExecutor pullExecutor;
    private final BlockingQueue<Runnable> pullRequestQueue = new LinkedBlockingQueue<Runnable>();
    private final MessageQueueListener messageQueueListener = new MessageQueueListenerImpl();

    class MessageQueueListenerImpl implements MessageQueueListener {
        @Override
        public void messageQueueChanged(String topic, Set<MessageQueue> mqAll, Set<MessageQueue> mqDivided) {
            MessageModel messageModel =
                    MQPullConsumerScheduleService.this.defaultMQPullConsumer.getMessageModel();
            switch (messageModel) {
            case BROADCASTING:
                break;
            case CLUSTERING:
                break;
            default:
                break;
            }
        }
    }


    public MQPullConsumerScheduleService(final DefaultMQPullConsumer defaultMQPullConsumer) {
        this.defaultMQPullConsumer = defaultMQPullConsumer;
        this.defaultMQPullConsumer.setMessageQueueListener(this.messageQueueListener);
    }


    /**
     * 启动服务
     * 
     * @throws MQClientException
     */
    public void start() throws MQClientException {
        final String group = this.defaultMQPullConsumer.getConsumerGroup();
        this.pullExecutor = new ThreadPoolExecutor(//
            this.pullThreadNums,//
            this.pullThreadNums,//
            1000 * 60,//
            TimeUnit.MILLISECONDS,//
            this.pullRequestQueue,//
            new ThreadFactory() {
                private AtomicLong threadIndex = new AtomicLong(0);


                @Override
                public Thread newThread(Runnable r) {
                    return new Thread(r, "PullMessageThread-" //
                            + group//
                            + "-" + this.threadIndex.incrementAndGet());
                }
            });
    }


    /**
     * 关闭服务
     */
    public void shutdown() {
        if (this.pullExecutor != null) {
            this.pullExecutor.shutdown();
        }
    }
}
