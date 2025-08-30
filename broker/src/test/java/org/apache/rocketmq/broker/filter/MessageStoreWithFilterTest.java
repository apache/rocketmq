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

package org.apache.rocketmq.broker.filter;

import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.filter.ExpressionType;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageExtBrokerInner;
import org.apache.rocketmq.remoting.protocol.heartbeat.SubscriptionData;
import org.apache.rocketmq.store.CommitLogDispatcher;
import org.apache.rocketmq.store.ConsumeQueueExt;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.DispatchRequest;
import org.apache.rocketmq.store.GetMessageResult;
import org.apache.rocketmq.store.GetMessageStatus;
import org.apache.rocketmq.store.MessageArrivingListener;
import org.apache.rocketmq.store.MessageFilter;
import org.apache.rocketmq.store.PutMessageResult;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.apache.rocketmq.store.stats.BrokerStatsManager;
import org.awaitility.core.ThrowingRunnable;
import org.junit.Assume;
import org.junit.Test;

import java.io.File;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

public class MessageStoreWithFilterTest {

    private static final String MSG = "Once, there was a chance for me!";
    private static final byte[] MSG_BODY = MSG.getBytes();

    private static final String TOPIC = "topic";
    private static final int QUEUE_ID = 0;
    private static final String BASE_STORE_PATH = System.getProperty("java.io.tmpdir") + File.separator + "unit_test_store";
    private static final int COMMIT_LOG_FILE_SIZE = 1024 * 1024 * 256;
    private static final int CQ_FILE_SIZE = 300000 * 20;
    private static final int CQ_EXT_FILE_SIZE = 300000 * 128;

    private static SocketAddress bornHost;
    private static SocketAddress storeHost;

    private final int topicCount = 3;
    private final int msgPerTopic = 30;

    static {
        try {
            storeHost = new InetSocketAddress(InetAddress.getLocalHost(), 8123);
        } catch (UnknownHostException ignored) {
        }
        try {
            bornHost = new InetSocketAddress(InetAddress.getByName("127.0.0.1"), 0);
        } catch (UnknownHostException ignored) {
        }
    }

    public MessageExtBrokerInner buildMessage() {
        MessageExtBrokerInner msg = new MessageExtBrokerInner();
        msg.setTopic(TOPIC);
        msg.setTags(System.currentTimeMillis() + "TAG");
        msg.setKeys("Hello");
        msg.setBody(MSG_BODY);
        msg.setKeys(String.valueOf(System.currentTimeMillis()));
        msg.setQueueId(QUEUE_ID);
        msg.setSysFlag(0);
        msg.setBornTimestamp(System.currentTimeMillis());
        msg.setStoreHost(storeHost);
        msg.setBornHost(bornHost);
        for (int i = 1; i < 3; i++) {
            msg.putUserProperty(String.valueOf(i), "imagoodperson" + i);
        }
        msg.setPropertiesString(MessageDecoder.messageProperties2String(msg.getProperties()));

        return msg;
    }

    public MessageStoreConfig buildStoreConfig(String storePath, int commitLogFileSize, int cqFileSize,
                                               boolean enableCqExt, int cqExtFileSize) {
        MessageStoreConfig messageStoreConfig = new MessageStoreConfig();
        messageStoreConfig.setMappedFileSizeCommitLog(commitLogFileSize);
        messageStoreConfig.setMappedFileSizeConsumeQueue(cqFileSize);
        messageStoreConfig.setMappedFileSizeConsumeQueueExt(cqExtFileSize);
        messageStoreConfig.setMessageIndexEnable(false);
        messageStoreConfig.setEnableConsumeQueueExt(enableCqExt);

        messageStoreConfig.setStorePathRootDir(storePath);
        messageStoreConfig.setStorePathCommitLog(storePath + File.separator + "commitlog");

        return messageStoreConfig;
    }

    protected DefaultMessageStore gen(String testName, ConsumerFilterManager filterManager) throws Exception {
        String storePath = BASE_STORE_PATH + "_" + testName;
        cleanStoreDirectory(storePath);

        MessageStoreConfig messageStoreConfig = buildStoreConfig(
                storePath, COMMIT_LOG_FILE_SIZE, CQ_FILE_SIZE, true, CQ_EXT_FILE_SIZE
        );

        BrokerConfig brokerConfig = new BrokerConfig();
        brokerConfig.setEnableCalcFilterBitMap(true);
        brokerConfig.setMaxErrorRateOfBloomFilter(20);
        brokerConfig.setExpectConsumerNumUseFilter(64);

        DefaultMessageStore master = new DefaultMessageStore(
                messageStoreConfig,
                new BrokerStatsManager(brokerConfig.getBrokerClusterName(), brokerConfig.isEnableDetailStat()),
                new MessageArrivingListener() {
                    @Override
                    public void arriving(String topic, int queueId, long logicOffset, long tagsCode,
                                         long msgStoreTime, byte[] filterBitMap, Map<String, String> properties) {
                    }
                }
                , brokerConfig, new ConcurrentHashMap<>());

        master.getDispatcherList().addFirst(new CommitLogDispatcher() {
            @Override
            public void dispatch(DispatchRequest request) {
                try {
                } catch (Throwable e) {
                    e.printStackTrace();
                }
            }
        });
        master.getDispatcherList().addFirst(new CommitLogDispatcherCalcBitMap(brokerConfig, filterManager));

        if (MixAll.isWindows()) {
            Assume.assumeTrue(master.load());
        } else {
            assertThat(master.load()).isTrue();
        }

        master.start();

        return master;
    }

    private void cleanStoreDirectory(String storePath) {
        File storeDir = new File(storePath);
        if (storeDir.exists()) {
            UtilAll.deleteFile(storeDir);
        }
    }

    public void destroy(DefaultMessageStore master) {
        if (master != null) {
            String storePath = master.getMessageStoreConfig().getStorePathRootDir();
            try {
                master.shutdown();
                master.destroy();
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                cleanStoreDirectory(storePath);
            }
        }
    }

    protected List<MessageExtBrokerInner> putMsg(DefaultMessageStore master, int topicCount,
                                                 int msgCountPerTopic) throws Exception {
        List<MessageExtBrokerInner> msgs = new ArrayList<>();
        for (int i = 0; i < topicCount; i++) {
            String realTopic = TOPIC + i;
            for (int j = 0; j < msgCountPerTopic; j++) {
                MessageExtBrokerInner msg = buildMessage();
                msg.setTopic(realTopic);
                msg.putUserProperty("a", String.valueOf(j * 10 + 5));
                msg.setPropertiesString(MessageDecoder.messageProperties2String(msg.getProperties()));

                PutMessageResult result = master.putMessage(msg);

                msg.setMsgId(result.getAppendMessageResult().getMsgId());

                msgs.add(msg);
            }
        }

        return msgs;
    }

    protected List<MessageExtBrokerInner> filtered(List<MessageExtBrokerInner> msgs, ConsumerFilterData filterData) {
        List<MessageExtBrokerInner> filteredMsgs = new ArrayList<>();

        for (MessageExtBrokerInner messageExtBrokerInner : msgs) {

            if (!messageExtBrokerInner.getTopic().equals(filterData.getTopic())) {
                continue;
            }

            try {
                Object evlRet = filterData.getCompiledExpression().evaluate(new MessageEvaluationContext(messageExtBrokerInner.getProperties()));

                if (evlRet == null || !(evlRet instanceof Boolean) || (Boolean) evlRet) {
                    filteredMsgs.add(messageExtBrokerInner);
                }
            } catch (Exception e) {
                e.printStackTrace();
                assertThat(true).isFalse();
            }
        }

        return filteredMsgs;
    }

    @Test
    public void testGetMessage_withFilterBitMapAndConsumerChanged() throws Exception {
        ConsumerFilterManager filterManager = ConsumerFilterManagerTest.gen(topicCount, msgPerTopic);
        DefaultMessageStore master = gen("testFilterBitMapAndConsumerChanged", filterManager);

        try {
            List<MessageExtBrokerInner> msgs = putMsg(master, topicCount, msgPerTopic);

            Thread.sleep(200);

            // reset consumer;
            String topic = "topic" + 0;
            String resetGroup = "CID_" + 2;
            String normalGroup = "CID_" + 3;

            {
                // reset CID_2@topic0 to get all messages.
                SubscriptionData resetSubData = new SubscriptionData();
                resetSubData.setExpressionType(ExpressionType.SQL92);
                resetSubData.setTopic(topic);
                resetSubData.setClassFilterMode(false);
                resetSubData.setSubString("a is not null OR a is null");

                ConsumerFilterData resetFilterData = ConsumerFilterManager.build(topic,
                        resetGroup, resetSubData.getSubString(), resetSubData.getExpressionType(),
                        System.currentTimeMillis());

                GetMessageResult resetGetResult = master.getMessage(resetGroup, topic, QUEUE_ID, 0, 1000,
                        new ExpressionMessageFilter(resetSubData, resetFilterData, filterManager));
                try {
                    assertThat(resetGetResult).isNotNull();

                    List<MessageExtBrokerInner> filteredMsgs = filtered(msgs, resetFilterData);

                    assertThat(resetGetResult.getMessageBufferList().size()).isEqualTo(filteredMsgs.size());
                } finally {
                    assert resetGetResult != null;
                    resetGetResult.release();
                }
            }

            {
                ConsumerFilterData normalFilterData = filterManager.get(topic, normalGroup);
                assertThat(normalFilterData).isNotNull();
                assertThat(normalFilterData.getBornTime()).isLessThan(System.currentTimeMillis());

                SubscriptionData normalSubData = new SubscriptionData();
                normalSubData.setExpressionType(normalFilterData.getExpressionType());
                normalSubData.setTopic(topic);
                normalSubData.setClassFilterMode(false);
                normalSubData.setSubString(normalFilterData.getExpression());

                List<MessageExtBrokerInner> filteredMsgs = filtered(msgs, normalFilterData);

                GetMessageResult normalGetResult = master.getMessage(normalGroup, topic, QUEUE_ID, 0, 1000,
                        new ExpressionMessageFilter(normalSubData, normalFilterData, filterManager));
                try {
                    assertThat(normalGetResult).isNotNull();
                    assertThat(normalGetResult.getMessageBufferList().size()).isEqualTo(filteredMsgs.size());
                } finally {
                    assert normalGetResult != null;
                    normalGetResult.release();
                }
            }
        } finally {
            destroy(master);
        }
    }

    @Test
    public void testGetMessage_withFilterBitMap() throws Exception {
        ConsumerFilterManager filterManager = ConsumerFilterManagerTest.gen(topicCount, msgPerTopic);
        DefaultMessageStore master = gen("testFilterBitMap", filterManager);

        try {
            List<MessageExtBrokerInner> msgs = putMsg(master, topicCount, msgPerTopic);

            Thread.sleep(100);

            for (int i = 0; i < topicCount; i++) {
                String realTopic = TOPIC + i;

                for (int j = 0; j < msgPerTopic; j++) {
                    String group = "CID_" + j;

                    ConsumerFilterData filterData = filterManager.get(realTopic, group);
                    assertThat(filterData).isNotNull();

                    List<MessageExtBrokerInner> filteredMsgs = filtered(msgs, filterData);

                    SubscriptionData subscriptionData = new SubscriptionData();
                    subscriptionData.setExpressionType(filterData.getExpressionType());
                    subscriptionData.setTopic(filterData.getTopic());
                    subscriptionData.setClassFilterMode(false);
                    subscriptionData.setSubString(filterData.getExpression());

                    GetMessageResult getMessageResult = master.getMessage(group, realTopic, QUEUE_ID, 0, 10000,
                            new ExpressionMessageFilter(subscriptionData, filterData, filterManager));
                    try {
                        assertThat(getMessageResult).isNotNull();
                        assertThat(GetMessageStatus.FOUND).isEqualTo(getMessageResult.getStatus());
                        assertThat(getMessageResult.getMessageBufferList()).isNotNull().isNotEmpty();
                        assertThat(getMessageResult.getMessageBufferList().size()).isEqualTo(filteredMsgs.size());

                        for (ByteBuffer buffer : getMessageResult.getMessageBufferList()) {
                            MessageExt messageExt = MessageDecoder.decode(buffer.slice(), false);
                            assertThat(messageExt).isNotNull();

                            Object evlRet = null;
                            try {
                                evlRet = filterData.getCompiledExpression().evaluate(new MessageEvaluationContext(messageExt.getProperties()));
                            } catch (Exception e) {
                                e.printStackTrace();
                                assertThat(true).isFalse();
                            }

                            assertThat(evlRet).isNotNull().isEqualTo(Boolean.TRUE);

                            // check
                            boolean find = false;
                            for (MessageExtBrokerInner messageExtBrokerInner : filteredMsgs) {
                                if (messageExtBrokerInner.getMsgId().equals(messageExt.getMsgId())) {
                                    find = true;
                                }
                            }
                            assertThat(find).isTrue();
                        }
                    } finally {
                        assert getMessageResult != null;
                        getMessageResult.release();
                    }
                }
            }
        } finally {
            destroy(master);
        }
    }

    @Test
    public void testGetMessage_withFilter_checkTagsCode() throws Exception {
        ConsumerFilterManager filterManager = ConsumerFilterManagerTest.gen(topicCount, msgPerTopic);
        DefaultMessageStore master = gen("testFilterCheckTagsCode", filterManager);

        try {
            putMsg(master, topicCount, msgPerTopic);

            await().atMost(3, TimeUnit.SECONDS).untilAsserted(new ThrowingRunnable() {
                @Override
                public void run() {
                    for (int i = 0; i < topicCount; i++) {
                        final String realTopic = TOPIC + i;
                        GetMessageResult getMessageResult = master.getMessage("test", realTopic, QUEUE_ID, 0, 10000,
                                new MessageFilter() {
                                    @Override
                                    public boolean isMatchedByConsumeQueue(Long tagsCode,
                                                                           ConsumeQueueExt.CqExtUnit cqExtUnit) {
                                        return tagsCode == null || tagsCode > ConsumeQueueExt.MAX_ADDR;
                                    }

                                    @Override
                                    public boolean isMatchedByCommitLog(ByteBuffer msgBuffer,
                                                                        Map<String, String> properties) {
                                        return true;
                                    }
                                });
                        assertThat(getMessageResult.getMessageCount()).isEqualTo(msgPerTopic);
                    }
                }
            });
        } finally {
            destroy(master);
        }
    }
}
