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
package org.apache.rocketmq.test.lmq.benchmark;

import com.google.common.math.IntMath;
import com.google.common.math.LongMath;
import java.nio.charset.StandardCharsets;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.client.consumer.DefaultMQPullConsumer;
import org.apache.rocketmq.client.consumer.PullCallback;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.client.consumer.PullStatus;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.header.QueryConsumerOffsetRequestHeader;
import org.apache.rocketmq.common.protocol.header.UpdateConsumerOffsetRequestHeader;
import org.apache.rocketmq.common.protocol.route.TopicRouteData;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.apache.rocketmq.test.util.StatUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
public class BenchLmqStore {
    private static Logger logger = LoggerFactory.getLogger(BenchLmqStore.class);
    private static String namesrv = System.getProperty("namesrv", "127.0.0.1:9876");
    private static String lmqTopic = System.getProperty("lmqTopic", "lmqTestTopic");
    private static boolean enableSub = Boolean.parseBoolean(System.getProperty("enableSub", "true"));
    private static String queuePrefix = System.getProperty("queuePrefix", "lmqTest");
    private static int tps = Integer.parseInt(System.getProperty("tps", "1"));
    private static int lmqNum = Integer.parseInt(System.getProperty("lmqNum", "1"));
    private static int sendThreadNum = Integer.parseInt(System.getProperty("sendThreadNum", "64"));
    private static int consumerThreadNum = Integer.parseInt(System.getProperty("consumerThreadNum", "64"));
    private static String brokerName = System.getProperty("brokerName", "broker-a");
    private static int size = Integer.parseInt(System.getProperty("size", "128"));
    private static int suspendTime = Integer.parseInt(System.getProperty("suspendTime", "2000"));
    private static final boolean RETRY_NO_MATCHED_MSG = Boolean.parseBoolean(System.getProperty("retry_no_matched_msg", "false"));
    private static boolean benchOffset = Boolean.parseBoolean(System.getProperty("benchOffset", "false"));
    private static int benchOffsetNum = Integer.parseInt(System.getProperty("benchOffsetNum", "1"));
    private static Map<MessageQueue, Long> offsetMap = new ConcurrentHashMap<>(256);
    private static Map<MessageQueue, Boolean> pullStatus = new ConcurrentHashMap<>(256);
    private static Map<Integer, Map<MessageQueue, Long>> pullEvent = new ConcurrentHashMap<>(256);
    public static DefaultMQProducer defaultMQProducer;
    private static int pullConsumerNum = Integer.parseInt(System.getProperty("pullConsumerNum", "8"));
    public static DefaultMQPullConsumer[] defaultMQPullConsumers = new DefaultMQPullConsumer[pullConsumerNum];
    private static AtomicLong rid = new AtomicLong();
    private static final String LMQ_PREFIX = "%LMQ%";

    public static void main(String[] args) throws InterruptedException, MQClientException, MQBrokerException,
        RemotingException {
        defaultMQProducer = new DefaultMQProducer();
        defaultMQProducer.setProducerGroup("PID_LMQ_TEST");
        defaultMQProducer.setVipChannelEnabled(false);
        defaultMQProducer.setNamesrvAddr(namesrv);
        defaultMQProducer.start();
        //defaultMQProducer.createTopic(lmqTopic, lmqTopic, 8);
        for (int i = 0; i < pullConsumerNum; i++) {
            DefaultMQPullConsumer defaultMQPullConsumer = new DefaultMQPullConsumer();
            defaultMQPullConsumers[i] = defaultMQPullConsumer;
            defaultMQPullConsumer.setNamesrvAddr(namesrv);
            defaultMQPullConsumer.setVipChannelEnabled(false);
            defaultMQPullConsumer.setConsumerGroup("CID_RMQ_SYS_LMQ_TEST_" + i);
            defaultMQPullConsumer.setInstanceName("CID_RMQ_SYS_LMQ_TEST_" + i);
            defaultMQPullConsumer.setRegisterTopics(new HashSet<>(Arrays.asList(lmqTopic)));
            defaultMQPullConsumer.setBrokerSuspendMaxTimeMillis(suspendTime);
            defaultMQPullConsumer.setConsumerTimeoutMillisWhenSuspend(suspendTime + 1000);
            defaultMQPullConsumer.start();
        }
        Thread.sleep(3000L);
        if (benchOffset) {
            doBenchOffset();
            return;
        }
        ScheduledThreadPoolExecutor consumerPool = new ScheduledThreadPoolExecutor(consumerThreadNum, new ThreadFactoryImpl("test"));
        for (int i = 0; i < consumerThreadNum; i++) {
            final int idx = i;
            consumerPool.scheduleWithFixedDelay(() -> {
                try {
                    Map<MessageQueue, Long> map = pullEvent.get(idx);
                    if (map == null) {
                        return;
                    }
                    for (Map.Entry<MessageQueue, Long> entry : map.entrySet()) {
                        try {
                            Boolean status = pullStatus.get(entry.getKey());
                            if (Boolean.TRUE.equals(status)) {
                                continue;
                            }
                            doPull(map, entry.getKey(), entry.getValue());
                        } catch (Exception e) {
                            logger.error("pull broker msg error", e);
                        }
                    }
                } catch (Exception e) {
                    logger.error("exec doPull task error", e);
                }
            }, 1, 1, TimeUnit.MILLISECONDS);
        }
        // init queue sub
        if (enableSub && lmqNum > 0 && StringUtils.isNotBlank(brokerName)) {
            for (int i = 0; i < lmqNum; i++) {
                long idx = rid.incrementAndGet();
                String queue = LMQ_PREFIX + queuePrefix + LongMath.mod(idx, lmqNum);
                MessageQueue mq = new MessageQueue(queue, brokerName, 0);
                int queueHash = IntMath.mod(queue.hashCode(), consumerThreadNum);
                pullEvent.putIfAbsent(queueHash, new ConcurrentHashMap<>());
                pullEvent.get(queueHash).put(mq, idx);
            }
        }
        Thread.sleep(5000L);
        doSend();
    }
    public static void doSend() {
        StringBuilder sb = new StringBuilder();
        for (int j = 0; j < size; j += 10) {
            sb.append("hello baby");
        }
        byte[] body = sb.toString().getBytes(StandardCharsets.UTF_8);
        String pubKey = "pub";
        ExecutorService sendPool = Executors.newFixedThreadPool(sendThreadNum);
        for (int i = 0; i < sendThreadNum; i++) {
            sendPool.execute(() -> {
                while (true) {
                    if (StatUtil.isOverFlow(pubKey, tps)) {
                        try {
                            Thread.sleep(100L);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                    long start = System.currentTimeMillis();
                    try {
                        long idx = rid.incrementAndGet();
                        Message message = new Message(lmqTopic, body);
                        String queue = lmqTopic;
                        if (lmqNum > 0) {
                            queue = LMQ_PREFIX + queuePrefix + idx % lmqNum;
                            message.putUserProperty("INNER_MULTI_DISPATCH", queue);
                        }
                        SendResult sendResult = defaultMQProducer.send(message);
                        StatUtil.addInvoke(pubKey, System.currentTimeMillis() - start);
                        if (StatUtil.nowTps(pubKey) < 10) {
                            logger.warn("pub: {} ", sendResult.getMsgId());
                        }
                        if (enableSub) {
                            MessageQueue mq = new MessageQueue(queue, sendResult.getMessageQueue().getBrokerName(),
                                    lmqNum > 0 ? 0 : sendResult.getMessageQueue().getQueueId());
                            int queueHash = IntMath.mod(queue.hashCode(), consumerThreadNum);
                            pullEvent.putIfAbsent(queueHash, new ConcurrentHashMap<>());
                            pullEvent.get(queueHash).put(mq, idx);
                        }
                    } catch (Exception e) {
                        logger.error("", e);
                        StatUtil.addInvoke(pubKey, System.currentTimeMillis() - start, false);
                    }
                }
            });
        }
    }
    public static void doPull(Map<MessageQueue, Long> eventMap, MessageQueue mq, Long eventId) throws RemotingException, InterruptedException, MQClientException {
        if (!enableSub) {
            eventMap.remove(mq, eventId);
            pullStatus.remove(mq);
            return;
        }
        DefaultMQPullConsumer defaultMQPullConsumer = defaultMQPullConsumers[(int) (eventId % pullConsumerNum)];
        Long offset = offsetMap.get(mq);
        if (offset == null) {
            long start = System.currentTimeMillis();
            offset = defaultMQPullConsumer.maxOffset(mq);
            StatUtil.addInvoke("maxOffset", System.currentTimeMillis() - start);
            offsetMap.put(mq, offset);
        }
        long start = System.currentTimeMillis();
        if (null != pullStatus.putIfAbsent(mq, true)) {
            return;
        }
        defaultMQPullConsumer.pullBlockIfNotFound(
                mq, "*", offset, 32,
                new PullCallback() {
                    @Override
                    public void onSuccess(PullResult pullResult) {
                        StatUtil.addInvoke(pullResult.getPullStatus().name(), System.currentTimeMillis() - start);
                        eventMap.remove(mq, eventId);
                        pullStatus.remove(mq);
                        offsetMap.put(mq, pullResult.getNextBeginOffset());
                        StatUtil.addInvoke("doPull", System.currentTimeMillis() - start);
                        if (PullStatus.NO_MATCHED_MSG.equals(pullResult.getPullStatus()) && RETRY_NO_MATCHED_MSG) {
                            long idx = rid.incrementAndGet();
                            eventMap.put(mq, idx);
                        }
                        List<MessageExt> list = pullResult.getMsgFoundList();
                        if (list == null || list.isEmpty()) {
                            StatUtil.addInvoke("NoMsg", System.currentTimeMillis() - start);
                            return;
                        }
                        for (MessageExt messageExt : list) {
                            StatUtil.addInvoke("sub", System.currentTimeMillis() - messageExt.getBornTimestamp());
                            if (StatUtil.nowTps("sub") < 10) {
                                logger.warn("sub: {}", messageExt.getMsgId());
                            }
                        }
                    }
                    @Override
                    public void onException(Throwable e) {
                        eventMap.remove(mq, eventId);
                        pullStatus.remove(mq);
                        logger.error("", e);
                        StatUtil.addInvoke("doPull", System.currentTimeMillis() - start, false);
                    }
                });
    }
    public static void doBenchOffset() throws RemotingException, InterruptedException, MQClientException {
        ExecutorService sendPool = Executors.newFixedThreadPool(sendThreadNum);
        Map<String, Long> offsetMap = new ConcurrentHashMap<>();
        String statKey = "benchOffset";
        TopicRouteData topicRouteData = defaultMQPullConsumers[0].getDefaultMQPullConsumerImpl().
                getRebalanceImpl().getmQClientFactory().getMQClientAPIImpl().
                getTopicRouteInfoFromNameServer(lmqTopic, 3000);
        HashMap<Long, String> brokerMap = topicRouteData.getBrokerDatas().get(0).getBrokerAddrs();
        if (brokerMap == null || brokerMap.isEmpty()) {
            return;
        }
        String brokerAddress = brokerMap.get(MixAll.MASTER_ID);
        for (int i = 0; i < sendThreadNum; i++) {
            final int flag = i;
            sendPool.execute(new Runnable() {
                @Override
                public void run() {
                    while (true) {
                        try {
                            if (StatUtil.isOverFlow(statKey, tps)) {
                                Thread.sleep(100L);
                            }
                            long start = System.currentTimeMillis();
                            DefaultMQPullConsumer defaultMQPullConsumer = defaultMQPullConsumers[(int) (rid.incrementAndGet() % pullConsumerNum)];
                            long id = rid.incrementAndGet();
                            String lmq = LMQ_PREFIX + queuePrefix + id % benchOffsetNum;
                            String lmqCid = LMQ_PREFIX + "GID_LMQ@@c" + flag + "-" + id % benchOffsetNum;
                            Long offset = offsetMap.get(lmq);
                            if (offset == null) {
                                offsetMap.put(lmq, 0L);
                            }
                            long newOffset1 = offsetMap.get(lmq) + 1;
                            UpdateConsumerOffsetRequestHeader updateHeader = new UpdateConsumerOffsetRequestHeader();
                            updateHeader.setTopic(lmq);
                            updateHeader.setConsumerGroup(lmqCid);
                            updateHeader.setQueueId(0);
                            updateHeader.setCommitOffset(newOffset1);
                            defaultMQPullConsumer
                                    .getDefaultMQPullConsumerImpl()
                                    .getRebalanceImpl()
                                    .getmQClientFactory()
                                    .getMQClientAPIImpl().updateConsumerOffset(brokerAddress, updateHeader, 1000);
                            QueryConsumerOffsetRequestHeader queryHeader = new QueryConsumerOffsetRequestHeader();
                            queryHeader.setTopic(lmq);
                            queryHeader.setConsumerGroup(lmqCid);
                            queryHeader.setQueueId(0);
                            long newOffset2 = defaultMQPullConsumer
                                    .getDefaultMQPullConsumerImpl()
                                    .getRebalanceImpl()
                                    .getmQClientFactory()
                                    .getMQClientAPIImpl()
                                    .queryConsumerOffset(brokerAddress, queryHeader, 1000);
                            offsetMap.put(lmq, newOffset2);
                            if (newOffset1 != newOffset2) {
                                StatUtil.addInvoke("ErrorOffset", 1);
                            }
                            StatUtil.addInvoke(statKey, System.currentTimeMillis() - start);
                        } catch (Exception e) {
                            logger.error("", e);
                        }
                    }
                }
            });
        }
    }
}