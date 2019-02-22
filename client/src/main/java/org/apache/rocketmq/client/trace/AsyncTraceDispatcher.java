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
package org.apache.rocketmq.client.trace;

import org.apache.rocketmq.client.common.ThreadLocalIndex;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.impl.consumer.DefaultMQPushConsumerImpl;
import org.apache.rocketmq.client.impl.producer.DefaultMQProducerImpl;
import org.apache.rocketmq.client.impl.producer.TopicPublishInfo;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.remoting.RPCHook;

import java.io.IOException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.UUID;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.Set;
import java.util.HashSet;


import static org.apache.rocketmq.client.trace.TraceConstants.TRACE_INSTANCE_NAME;

public class AsyncTraceDispatcher implements TraceDispatcher {

    private final static InternalLogger log = ClientLogger.getLog();
    private final int queueSize;
    private final int batchSize;
    private final int maxMsgSize;
    private final DefaultMQProducer traceProducer;
    private final ThreadPoolExecutor traceExecuter;
    // The last discard number of log
    private AtomicLong discardCount;
    private Thread worker;
    private ArrayBlockingQueue<TraceContext> traceContextQueue;
    private ArrayBlockingQueue<Runnable> appenderQueue;
    private volatile Thread shutDownHook;
    private volatile boolean stopped = false;
    private DefaultMQProducerImpl hostProducer;
    private DefaultMQPushConsumerImpl hostConsumer;
    private volatile ThreadLocalIndex sendWhichQueue = new ThreadLocalIndex();
    private String dispatcherId = UUID.randomUUID().toString();
    private String traceTopicName;
    private AtomicBoolean isStarted = new AtomicBoolean(false);


    public AsyncTraceDispatcher(String traceTopicName, RPCHook rpcHook) throws MQClientException {
        // queueSize is greater than or equal to the n power of 2 of value
        this.queueSize = 2048;
        this.batchSize = 100;
        this.maxMsgSize = 128000;
        this.discardCount = new AtomicLong(0L);
        this.traceContextQueue = new ArrayBlockingQueue<TraceContext>(1024);
        this.appenderQueue = new ArrayBlockingQueue<Runnable>(queueSize);
        /**
         * 发送tipic为RMQ_SYS_TRACE_TOPIC（默认）
         */
        if (!UtilAll.isBlank(traceTopicName)) {
            this.traceTopicName = traceTopicName;
        } else {
            this.traceTopicName = MixAll.RMQ_SYS_TRACE_TOPIC;
        }
        this.traceExecuter = new ThreadPoolExecutor(//
                10, //
                20, //
                1000 * 60, //
                TimeUnit.MILLISECONDS, //
                this.appenderQueue, //
                new ThreadFactoryImpl("MQTraceSendThread_"));
        /**
         * 消息轨迹的生产者  默认组名为_INNER_TRACE_PRODUCER
         */
        traceProducer = getAndCreateTraceProducer(rpcHook);
    }

    public String getTraceTopicName() {
        return traceTopicName;
    }

    public void setTraceTopicName(String traceTopicName) {
        this.traceTopicName = traceTopicName;
    }

    public DefaultMQProducer getTraceProducer() {
        return traceProducer;
    }

    public DefaultMQProducerImpl getHostProducer() {
        return hostProducer;
    }

    public void setHostProducer(DefaultMQProducerImpl hostProducer) {
        this.hostProducer = hostProducer;
    }

    public DefaultMQPushConsumerImpl getHostConsumer() {
        return hostConsumer;
    }

    public void setHostConsumer(DefaultMQPushConsumerImpl hostConsumer) {
        this.hostConsumer = hostConsumer;
    }

    public void start(String nameSrvAddr) throws MQClientException {
        if (isStarted.compareAndSet(false, true)) {
            traceProducer.setNamesrvAddr(nameSrvAddr);
            traceProducer.setInstanceName(TRACE_INSTANCE_NAME + "_" + nameSrvAddr);
            traceProducer.start();
        }
        /**
         * 工作线程实例化并启动   AsyncRunnable
         */
        this.worker = new Thread(new AsyncRunnable(), "MQ-AsyncTraceDispatcher-Thread-" + dispatcherId);
        this.worker.setDaemon(true);
        this.worker.start();
        this.registerShutDownHook();
    }

    /**
     * 消息轨迹的生产者
     * @param rpcHook
     * @return
     */
    private DefaultMQProducer getAndCreateTraceProducer(RPCHook rpcHook) {
        DefaultMQProducer traceProducerInstance = this.traceProducer;
        if (traceProducerInstance == null) {
            traceProducerInstance = new DefaultMQProducer(rpcHook);
            /**
             * _INNER_TRACE_PRODUCER
             */
            traceProducerInstance.setProducerGroup(TraceConstants.GROUP_NAME);
            traceProducerInstance.setSendMsgTimeout(5000);
            traceProducerInstance.setVipChannelEnabled(false);
            // The max size of message is 128K
            traceProducerInstance.setMaxMessageSize(maxMsgSize - 10 * 1000);
        }
        return traceProducerInstance;
    }

    /**
     * 注入traceContextQueue
     * @param ctx data infomation
     * @return
     */
    @Override
    public boolean append(final Object ctx) {
        boolean result = traceContextQueue.offer((TraceContext) ctx);
        if (!result) {
            log.info("buffer full" + discardCount.incrementAndGet() + " ,context is " + ctx);
        }
        return result;
    }

    @Override
    public void flush() throws IOException {
        // The maximum waiting time for refresh,avoid being written all the time, resulting in failure to return.
        long end = System.currentTimeMillis() + 500;
        while (traceContextQueue.size() > 0 || appenderQueue.size() > 0 && System.currentTimeMillis() <= end) {
            try {
                Thread.sleep(1);
            } catch (InterruptedException e) {
                break;
            }
        }
        log.info("------end trace send " + traceContextQueue.size() + "   " + appenderQueue.size());
    }

    @Override
    public void shutdown() {
        this.stopped = true;
        this.traceExecuter.shutdown();
        if (isStarted.get()) {
            traceProducer.shutdown();
        }
        this.removeShutdownHook();
    }

    public void registerShutDownHook() {
        if (shutDownHook == null) {
            shutDownHook = new Thread(new Runnable() {
                private volatile boolean hasShutdown = false;

                @Override
                public void run() {
                    synchronized (this) {
                        if (!this.hasShutdown) {
                            try {
                                flush();
                            } catch (IOException e) {
                                log.error("system MQTrace hook shutdown failed ,maybe loss some trace data");
                            }
                        }
                    }
                }
            }, "ShutdownHookMQTrace");
            Runtime.getRuntime().addShutdownHook(shutDownHook);
        }
    }

    public void removeShutdownHook() {
        if (shutDownHook != null) {
            Runtime.getRuntime().removeShutdownHook(shutDownHook);
        }
    }

    class AsyncRunnable implements Runnable {
        private boolean stopped;

        @Override
        public void run() {
            while (!stopped) {
                List<TraceContext> contexts = new ArrayList<TraceContext>(batchSize);
                /**
                 * 每次批量执行100条数据（最大）
                 * 将TraceContext从traceContextQueue中取出  并注入到contexts中
                 */
                for (int i = 0; i < batchSize; i++) {
                    TraceContext context = null;
                    try {
                        //get trace data element from blocking Queue — traceContextQueue
                        context = traceContextQueue.poll(5, TimeUnit.MILLISECONDS);
                    } catch (InterruptedException e) {
                    }
                    if (context != null) {
                        contexts.add(context);
                    } else {
                        break;
                    }
                }

                /**
                 * 消息轨迹数据发送
                 */
                if (contexts.size() > 0) {
                    AsyncAppenderRequest request = new AsyncAppenderRequest(contexts);
                    traceExecuter.submit(request);
                } else if (AsyncTraceDispatcher.this.stopped) {
                    this.stopped = true;
                }
            }

        }
    }

    class AsyncAppenderRequest implements Runnable {
        List<TraceContext> contextList;

        public AsyncAppenderRequest(final List<TraceContext> contextList) {
            if (contextList != null) {
                this.contextList = contextList;
            } else {
                this.contextList = new ArrayList<TraceContext>(1);
            }
        }

        /**
         * 消息轨迹数据发送
         */
        @Override
        public void run() {
            sendTraceData(contextList);
        }

        /**
         * 消息轨迹数据发送
         * @param contextList
         */
        public void sendTraceData(List<TraceContext> contextList) {
            Map<String, List<TraceTransferBean>> transBeanMap = new HashMap<String, List<TraceTransferBean>>();
            for (TraceContext context : contextList) {
                if (context.getTraceBeans().isEmpty()) {
                    continue;
                }
                // Topic value corresponding to original message entity content
                String topic = context.getTraceBeans().get(0).getTopic();
                // Use  original message entity's topic as key
                String key = topic;
                List<TraceTransferBean> transBeanList = transBeanMap.get(key);
                if (transBeanList == null) {
                    transBeanList = new ArrayList<TraceTransferBean>();
                    transBeanMap.put(key, transBeanList);
                }
                /**
                 * 实例化TraceTransferBean
                 */
                TraceTransferBean traceData = TraceDataEncoder.encoderFromContextBean(context);
                transBeanList.add(traceData);
            }
            for (Map.Entry<String, List<TraceTransferBean>> entry : transBeanMap.entrySet()) {
                /**
                 * 发送消息
                 */
                flushData(entry.getValue());
            }
        }

        /**
         * Batch sending data actually
         */
        private void flushData(List<TraceTransferBean> transBeanList) {
            if (transBeanList.size() == 0) {
                return;
            }
            // Temporary buffer
            StringBuilder buffer = new StringBuilder(1024);
            int count = 0;
            Set<String> keySet = new HashSet<String>();

            for (TraceTransferBean bean : transBeanList) {
                // Keyset of message trace includes msgId of or original message
                keySet.addAll(bean.getTransKey());
                buffer.append(bean.getTransData());
                count++;
                // Ensure that the size of the package should not exceed the upper limit.
                /**
                 * 数据大于等于4M  则将现有数据全部发送  并清空缓存   继续下一个循环
                 */
                if (buffer.length() >= traceProducer.getMaxMessageSize()) {
                    /**
                     * 发送消息
                     */
                    sendTraceDataByMQ(keySet, buffer.toString());
                    // Clear temporary buffer after finishing
                    /**
                     * 清缓存
                     */
                    buffer.delete(0, buffer.length());
                    keySet.clear();
                    count = 0;
                }
            }

            /**
             * 有待发送数据  则发送
             */
            if (count > 0) {
                sendTraceDataByMQ(keySet, buffer.toString());
            }
            transBeanList.clear();
        }

        /**
         * Send message trace data
         *
         * @param keySet the keyset in this batch(including msgId in original message not offsetMsgId)
         * @param data   the message trace data in this batch
         */
        private void sendTraceDataByMQ(Set<String> keySet, final String data) {
            String topic = traceTopicName;
            final Message message = new Message(topic, data.getBytes());

            // Keyset of message trace includes msgId of or original message
            message.setKeys(keySet);
            /**
             * 组装后的消息为
             * Message{topic='RMQ_SYS_TRACE_TOPIC', flag=0, properties={KEYS=1550819829391 1550819829181 1550819829390 C0A8326B1E0018B4AAC26F6D3A900004 C0A8326B1E0018B4AAC26F6D3A3D0000 C0A8326B1E0018B4AAC26F6D3A8E0002, WAIT=true}, body=[80, 117, 98, 1, 49, 53, 53, 48, 56, 49, 57, 56, 50, 57, 51, 49, 48, 1, 68, 101, 102, 97, 117, 108, 116, 82, 101, 103, 105, 111, 110, 1, 116, 114, 97, 99, 101, 77, 101, 115, 115, 97, 103, 101, 80, 114, 111, 100, 117, 99, 101, 114, 1, 109, 101, 115, 115, 97, 103, 101, 84, 114, 97, 99, 101, 1, 67, 48, 65, 56, 51, 50, 54, 66, 49, 69, 48, 48, 49, 56, 66, 52, 65, 65, 67, 50, 54, 70, 54, 68, 51, 65, 51, 68, 48, 48, 48, 48, 1, 84, 97, 103, 65, 1, 49, 53, 53, 48, 56, 49, 57, 56, 50, 57, 49, 56, 49, 1, 49, 57, 50, 46, 49, 54, 56, 46, 53, 48, 46, 49, 48, 55, 58, 49, 48, 57, 48, 57, 1, 49, 49, 1, 55, 57, 1, 48, 1, 67, 48, 65, 56, 51, 50, 54, 66, 48, 48, 48, 48, 50, 65, 57, 70, 48, 48, 48, 48, 48, 48, 48, 48, 48, 51, 49, 57, 48, 55, 53, 67, 1, 116, 114, 117, 101, 2, 80, 117, 98, 1, 49, 53, 53, 48, 56, 49, 57, 56, 50, 57, 51, 57, 48, 1, 68, 101, 102, 97, 117, 108, 116, 82, 101, 103, 105, 111, 110, 1, 116, 114, 97, 99, 101, 77, 101, 115, 115, 97, 103, 101, 80, 114, 111, 100, 117, 99, 101, 114, 1, 109, 101, 115, 115, 97, 103, 101, 84, 114, 97, 99, 101, 1, 67, 48, 65, 56, 51, 50, 54, 66, 49, 69, 48, 48, 49, 56, 66, 52, 65, 65, 67, 50, 54, 70, 54, 68, 51, 65, 56, 69, 48, 48, 48, 50, 1, 84, 97, 103, 65, 1, 49, 53, 53, 48, 56, 49, 57, 56, 50, 57, 51, 57, 48, 1, 49, 57, 50, 46, 49, 54, 56, 46, 53, 48, 46, 49, 48, 55, 58, 49, 48, 57, 48, 57, 1, 49, 49, 1, 49, 1, 48, 1, 67, 48, 65, 56, 51, 50, 54, 66, 48, 48, 48, 48, 50, 65, 57, 70, 48, 48, 48, 48, 48, 48, 48, 48, 48, 51, 49, 57, 48, 56, 49, 70, 1, 116, 114, 117, 101, 2, 80, 117, 98, 1, 49, 53, 53, 48, 56, 49, 57, 56, 50, 57, 51, 57, 50, 1, 68, 101, 102, 97, 117, 108, 116, 82, 101, 103, 105, 111, 110, 1, 116, 114, 97, 99, 101, 77, 101, 115, 115, 97, 103, 101, 80, 114, 111, 100, 117, 99, 101, 114, 1, 109, 101, 115, 115, 97, 103, 101, 84, 114, 97, 99, 101, 1, 67, 48, 65, 56, 51, 50, 54, 66, 49, 69, 48, 48, 49, 56, 66, 52, 65, 65, 67, 50, 54, 70, 54, 68, 51, 65, 57, 48, 48, 48, 48, 52, 1, 84, 97, 103, 65, 1, 49, 53, 53, 48, 56, 49, 57, 56, 50, 57, 51, 57, 49, 1, 49, 57, 50, 46, 49, 54, 56, 46, 53, 48, 46, 49, 48, 55, 58, 49, 48, 57, 48, 57, 1, 49, 49, 1, 50, 1, 48, 1, 67, 48, 65, 56, 51, 50, 54, 66, 48, 48, 48, 48, 50, 65, 57, 70, 48, 48, 48, 48, 48, 48, 48, 48, 48, 51, 49, 57, 48, 56, 69, 50, 1, 116, 114, 117, 101, 2], transactionId='null'}
             */
            try {
                Set<String> traceBrokerSet = tryGetMessageQueueBrokerSet(traceProducer.getDefaultMQProducerImpl(), topic);
                SendCallback callback = new SendCallback() {
                    @Override
                    public void onSuccess(SendResult sendResult) {
                    }

                    @Override
                    public void onException(Throwable e) {
                        log.info("send trace data ,the traceData is " + data);
                    }
                };
                if (traceBrokerSet.isEmpty()) {
                    // No cross set
                    /**
                     * 发送
                     */
                    traceProducer.send(message, callback, 5000);
                } else {
                    /**
                     * 发送
                     */
                    traceProducer.send(message, new MessageQueueSelector() {
                        @Override
                        public MessageQueue select(List<MessageQueue> mqs, Message msg, Object arg) {
                            Set<String> brokerSet = (Set<String>) arg;
                            List<MessageQueue> filterMqs = new ArrayList<MessageQueue>();
                            for (MessageQueue queue : mqs) {
                                if (brokerSet.contains(queue.getBrokerName())) {
                                    filterMqs.add(queue);
                                }
                            }
                            int index = sendWhichQueue.getAndIncrement();
                            int pos = Math.abs(index) % filterMqs.size();
                            if (pos < 0) {
                                pos = 0;
                            }
                            return filterMqs.get(pos);
                        }
                    }, traceBrokerSet, callback);
                }

            } catch (Exception e) {
                log.info("send trace data,the traceData is" + data);
            }
        }

        /**
         * 获取topic对应的brokername
         * @param producer
         * @param topic
         * @return
         */
        private Set<String> tryGetMessageQueueBrokerSet(DefaultMQProducerImpl producer, String topic) {
            Set<String> brokerSet = new HashSet<String>();
            /**
             * 获取发布信息
             */
            TopicPublishInfo topicPublishInfo = producer.getTopicPublishInfoTable().get(topic);
            if (null == topicPublishInfo || !topicPublishInfo.ok()) {
                producer.getTopicPublishInfoTable().putIfAbsent(topic, new TopicPublishInfo());
                /**
                 * 访问NameServer  获取topic对应的发布信息
                 */
                producer.getmQClientFactory().updateTopicRouteInfoFromNameServer(topic);
                topicPublishInfo = producer.getTopicPublishInfoTable().get(topic);
            }
            if (topicPublishInfo.isHaveTopicRouterInfo() || topicPublishInfo.ok()) {
                for (MessageQueue queue : topicPublishInfo.getMessageQueueList()) {
                    brokerSet.add(queue.getBrokerName());
                }
            }
            return brokerSet;
        }
    }

}
