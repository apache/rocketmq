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
package org.apache.rocketmq.client.trace.core.dispatch.impl;

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
import org.apache.rocketmq.client.trace.core.common.TrackTraceConstants;
import org.apache.rocketmq.client.trace.core.common.TrackTraceContext;
import org.apache.rocketmq.client.trace.core.common.TrackTraceDataEncoder;
import org.apache.rocketmq.client.trace.core.common.TrackTraceTransferBean;
import org.apache.rocketmq.client.trace.core.dispatch.AsyncDispatcher;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.logging.InternalLogger;

import java.io.IOException;
import java.util.List;
import java.util.HashMap;
import java.util.UUID;
import java.util.Properties;
import java.util.Set;
import java.util.HashSet;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by zongtanghu on 2018/11/6.
 */
public class AsyncArrayDispatcher implements AsyncDispatcher {

    private final static InternalLogger log = ClientLogger.getLog();
    private final int queueSize;
    private final int batchSize;
    private final DefaultMQProducer traceProducer;
    private final ThreadPoolExecutor traceExecuter;
    // the last discard number of log
    private AtomicLong discardCount;
    private Thread worker;
    private ArrayBlockingQueue<TrackTraceContext> traceContextQueue;
    private ArrayBlockingQueue<Runnable> appenderQueue;
    private volatile Thread shutDownHook;
    private volatile boolean stopped = false;
    private String dispatcherType;
    private DefaultMQProducerImpl hostProducer;
    private DefaultMQPushConsumerImpl hostConsumer;
    private volatile ThreadLocalIndex sendWhichQueue = new ThreadLocalIndex();
    private String dispatcherId = UUID.randomUUID().toString();

    public AsyncArrayDispatcher(Properties properties) throws MQClientException {
        dispatcherType = properties.getProperty(TrackTraceConstants.TRACE_DISPATCHER_TYPE);
        int queueSize = Integer.parseInt(properties.getProperty(TrackTraceConstants.ASYNC_BUFFER_SIZE, "2048"));
        // queueSize is greater than or equal to the n power of 2 of value
        queueSize = 1 << (32 - Integer.numberOfLeadingZeros(queueSize - 1));
        this.queueSize = queueSize;
        batchSize = Integer.parseInt(properties.getProperty(TrackTraceConstants.MAX_BATCH_NUM, "1"));
        this.discardCount = new AtomicLong(0L);
        traceContextQueue = new ArrayBlockingQueue<TrackTraceContext>(1024);
        appenderQueue = new ArrayBlockingQueue<Runnable>(queueSize);

        this.traceExecuter = new ThreadPoolExecutor(//
            10, //
            20, //
            1000 * 60, //
            TimeUnit.MILLISECONDS, //
            this.appenderQueue, //
            new ThreadFactoryImpl("MQTraceSendThread_"));
        traceProducer = TrackTraceProducerFactory.getTraceDispatcherProducer(properties);
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

    public void start(Properties properties) throws MQClientException {
        TrackTraceProducerFactory.registerTraceDispatcher(dispatcherId,properties.getProperty(TrackTraceConstants.NAMESRV_ADDR));
        this.worker = new Thread(new AsyncRunnable(), "MQ-AsyncArrayDispatcher-Thread-" + dispatcherId);
        this.worker.setDaemon(true);
        this.worker.start();
        this.registerShutDownHook();
    }

    @Override
    public boolean append(final Object ctx) {
        boolean result = traceContextQueue.offer((TrackTraceContext) ctx);
        if (!result) {
            log.info("buffer full" + discardCount.incrementAndGet() + " ,context is " + ctx);
        }
        return result;
    }

    @Override
    public void flush() throws IOException {
        // the maximum waiting time for refresh,avoid being written all the time, resulting in failure to return.
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
        TrackTraceProducerFactory.unregisterTraceDispatcher(dispatcherId);
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
                List<TrackTraceContext> contexts = new ArrayList<TrackTraceContext>(batchSize);
                for (int i = 0; i < batchSize; i++) {
                    TrackTraceContext context = null;
                    try {
                        //get track trace data element from blocking Queue — traceContextQueue
                        context = traceContextQueue.poll(5, TimeUnit.MILLISECONDS);
                    } catch (InterruptedException e) {
                    }
                    if (context != null) {
                        contexts.add(context);
                    } else {
                        break;
                    }
                }
                if (contexts.size() > 0) {
                    AsyncAppenderRequest request = new AsyncAppenderRequest(contexts);
                    traceExecuter.submit(request);
                } else if (AsyncArrayDispatcher.this.stopped) {
                    this.stopped = true;
                }
            }

        }
    }

    class AsyncAppenderRequest implements Runnable {
        List<TrackTraceContext> contextList;

        public AsyncAppenderRequest(final List<TrackTraceContext> contextList) {
            if (contextList != null) {
                this.contextList = contextList;
            } else {
                this.contextList = new ArrayList<TrackTraceContext>(1);
            }
        }

        @Override
        public void run() {
            sendTraceData(contextList);
        }
        
        public void sendTraceData(List<TrackTraceContext> contextList) {
            Map<String, List<TrackTraceTransferBean>> transBeanMap = new HashMap<String, List<TrackTraceTransferBean>>();
            for (TrackTraceContext context : contextList) {
                if (context.getTraceBeans().isEmpty()) {
                    continue;
                }
                //1.topic value corresponding to original message entity content
                String topic = context.getTraceBeans().get(0).getTopic();
                //2.use  original message entity's topic as key
                String key = topic;
                List<TrackTraceTransferBean> transBeanList = transBeanMap.get(key);
                if (transBeanList == null) {
                    transBeanList = new ArrayList<TrackTraceTransferBean>();
                    transBeanMap.put(key, transBeanList);
                }
                TrackTraceTransferBean traceData = TrackTraceDataEncoder.encoderFromContextBean(context);
                transBeanList.add(traceData);
            }
            for (Map.Entry<String, List<TrackTraceTransferBean>> entry : transBeanMap.entrySet()) {
                //key -> dataTopic(Not trace Topic)
                String dataTopic =  entry.getKey();
                flushData(entry.getValue(), dataTopic);
            }
        }

        /**
         * batch sending data actually
         */
        private void flushData(List<TrackTraceTransferBean> transBeanList, String topic) {
            if (transBeanList.size() == 0) {
                return;
            }
            // temporary buffer
            StringBuilder buffer = new StringBuilder(1024);
            int count = 0;
            Set<String> keySet = new HashSet<String>();

            for (TrackTraceTransferBean bean : transBeanList) {
                // keyset of message track trace includes msgId of or original message
                keySet.addAll(bean.getTransKey());
                buffer.append(bean.getTransData());
                count++;
                // Ensure that the size of the package should not exceed the upper limit.
                if (buffer.length() >= traceProducer.getMaxMessageSize()) {
                    sendTraceDataByMQ(keySet, buffer.toString());
                    // clear temporary buffer after finishing
                    buffer.delete(0, buffer.length());
                    keySet.clear();
                    count = 0;
                }
            }
            if (count > 0) {
                sendTraceDataByMQ(keySet, buffer.toString());
            }
            transBeanList.clear();
        }

        /**
         * send message track trace data
         *
         * @param keySet the keyset in this batch(including msgId in original message not offsetMsgId)
         * @param data   the message track trace data in this batch
         */
        private void sendTraceDataByMQ(Set<String> keySet, final String data) {
            String topic = TrackTraceConstants.TRACE_TOPIC;
            final Message message = new Message(topic, data.getBytes());

            //keyset of message track trace includes msgId of or original message
            message.setKeys(keySet);
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
                    //no cross set
                    traceProducer.send(message, callback, 5000);
                } else {
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

        private Set<String> tryGetMessageQueueBrokerSet(DefaultMQProducerImpl producer, String topic) {
            Set<String> brokerSet = new HashSet<String>();
            TopicPublishInfo topicPublishInfo = producer.getTopicPublishInfoTable().get(topic);
            if (null == topicPublishInfo || !topicPublishInfo.ok()) {
                producer.getTopicPublishInfoTable().putIfAbsent(topic, new TopicPublishInfo());
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
