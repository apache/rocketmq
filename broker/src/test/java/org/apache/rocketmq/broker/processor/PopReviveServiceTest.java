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
package org.apache.rocketmq.broker.processor;

import com.alibaba.fastjson.JSON;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.util.HookUtils;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.PopAckConstants;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExtBrokerInner;
import org.apache.rocketmq.common.utils.DataConverter;
import org.apache.rocketmq.remoting.netty.NettyClientConfig;
import org.apache.rocketmq.remoting.netty.NettyServerConfig;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.MessageArrivingListener;
import org.apache.rocketmq.store.PutMessageResult;
import org.apache.rocketmq.store.PutMessageStatus;
import org.apache.rocketmq.store.config.FlushDiskType;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.apache.rocketmq.store.pop.AckMsg;
import org.apache.rocketmq.store.pop.PopCheckPoint;
import org.apache.rocketmq.store.stats.BrokerStatsManager;
import org.apache.rocketmq.store.timer.TimerCheckpoint;
import org.apache.rocketmq.store.timer.TimerMessageStore;
import org.apache.rocketmq.store.timer.TimerMetrics;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Spy;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.File;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertTrue;

@RunWith(MockitoJUnitRunner.class)
public class PopReviveServiceTest {
    @Spy
    private BrokerController brokerController = new BrokerController(new BrokerConfig(), new NettyServerConfig(),
        new NettyClientConfig(), new MessageStoreConfig());
    private PopMessageProcessor popMessageProcessor;
    private AckMessageProcessor ackMessageProcessor;

    public static MessageStoreConfig storeConfig;
    private DefaultMessageStore messageStore;
    private String group = "FooBarGroup";
    private String topic = "FooBar";
    private String brokerName = "broker";

    @Before
    public void init() throws Exception {
        // Create messageStore for saving Ck, Ack messages.
        String baseDir = createBaseDir();
        storeConfig = new MessageStoreConfig();
        storeConfig.setMappedFileSizeCommitLog(1024 * 1024 * 1024);
        storeConfig.setMappedFileSizeTimerLog(1024 * 1024 * 1024);
        storeConfig.setMappedFileSizeConsumeQueue(10240);
        storeConfig.setMaxHashSlotNum(10000);
        storeConfig.setMaxIndexNum(100 * 1000);
        storeConfig.setStorePathRootDir(baseDir);
        storeConfig.setStorePathCommitLog(baseDir + File.separator + "commitlog");
        storeConfig.setFlushDiskType(FlushDiskType.ASYNC_FLUSH);
        storeConfig.setTimerInterceptDelayLevel(true);
        storeConfig.setTimerPrecisionMs(1000);
        TimerCheckpoint timerCheckpoint = new TimerCheckpoint(
            baseDir + File.separator + "config" + File.separator + "timercheck");
        TimerMetrics timerMetrics = new TimerMetrics(
            baseDir + File.separator + "config" + File.separator + "timermetrics");
        messageStore = new DefaultMessageStore(storeConfig, new BrokerStatsManager("TimerTest", false),
            new MyMessageArrivingListener(), new BrokerConfig());
        TimerMessageStore timerMessageStore = new TimerMessageStore(messageStore, storeConfig, timerCheckpoint,
            timerMetrics, null);
        messageStore.setTimerMessageStore(timerMessageStore);
        boolean load = messageStore.load();
        load = load && timerMessageStore.load();
        assertTrue(load);

        FieldUtils.writeField(brokerController.getBrokerConfig(), "enablePopBufferMerge", true, true);
        brokerController.setMessageStore(messageStore);
        brokerController.setTimerMessageStore(timerMessageStore);
        brokerController.getMessageStore().start();
        brokerController.getTimerMessageStore().start();

        popMessageProcessor = new PopMessageProcessor(brokerController);
        ackMessageProcessor = new AckMessageProcessor(brokerController);
        ackMessageProcessor.startPopReviveService();
    }

    @Test
    public void testRevive() throws Exception {
        PopBufferMergeService popBufferMergeService = new PopBufferMergeService(brokerController, popMessageProcessor);
        popBufferMergeService.start();
        PopCheckPoint ck = new PopCheckPoint();
        ck.setBitMap(0);
        int msgCnt = 1;
        ck.setNum((byte) msgCnt);
        long popTime = System.currentTimeMillis() - 1000;
        ck.setPopTime(popTime);
        int ckInvisibleTime = 8_000;
        int ackInvisibleTime = 10_000;
        ck.setInvisibleTime(ckInvisibleTime);
        int offset = -1;
        ck.setStartOffset(offset);
        ck.setCId(group);
        ck.setTopic(topic);
        int queueId = 0;
        ck.setQueueId((byte) queueId);

        int reviveQid = 0;
        long nextBeginOffset = 0L;
        long ackOffset = offset;
        AckMsg ackMsg = new AckMsg();
        ackMsg.setAckOffset(ackOffset);
        ackMsg.setStartOffset(offset);
        ackMsg.setConsumerGroup(group);
        ackMsg.setTopic(topic);
        ackMsg.setQueueId(queueId);
        ackMsg.setPopTime(popTime);
        ackMsg.setBrokerName(brokerName);
        try {
            assertThat(popBufferMergeService.addCk(ck, reviveQid, ackOffset, nextBeginOffset)).isTrue();
            for (PopBufferMergeService.PopCheckPointWrapper wrapper : popBufferMergeService.buffer.values()) {
                assertThat(wrapper.isCkStored()).isFalse();
                boolean putCkResult = putCkToStore(wrapper, ckInvisibleTime);
                assertThat(putCkResult).isTrue();
                assertThat(wrapper.isCkStored()).isTrue();
                boolean putAckResult = putAckToStore(wrapper, ackMsg, reviveQid, ackInvisibleTime);
                assertThat(putAckResult).isTrue();
            }
            long offsetOld = brokerController.getConsumerOffsetManager().queryOffset(PopAckConstants.REVIVE_GROUP,
                popMessageProcessor.reviveTopic, queueId);
            assertThat(offsetOld).isEqualTo(-1L);

            await().atMost(12, SECONDS).until(new Callable() {
                @Override
                public Boolean call() throws Exception {
                    long offsetNew = brokerController.getConsumerOffsetManager().queryOffset(
                        PopAckConstants.REVIVE_GROUP,
                        popMessageProcessor.reviveTopic, queueId);
                    return offsetNew == ackOffset + 1;

                }
            });

            System.out.println("Done...");

        } catch (Exception e) {
            System.out.printf("Error!" + e);
        }
    }

    public static String createBaseDir() {
        String baseDir = System.getProperty("user.home") + File.separator + "unitteststore-" + UUID.randomUUID();
        final File file = new File(baseDir);
        if (file.exists()) {
            System.exit(1);
        }
        return baseDir;
    }

    private class MyMessageArrivingListener implements MessageArrivingListener {
        @Override
        public void arriving(String topic, int queueId, long logicOffset, long tagsCode, long msgStoreTime,
            byte[] filterBitMap, Map<String, String> properties) {
        }
    }

    private boolean putCkToStore(final PopBufferMergeService.PopCheckPointWrapper pointWrapper,
        final long invisibleTime) {
        if (pointWrapper.getReviveQueueOffset() >= 0) {
            return false;
        }
        MessageExtBrokerInner msgInner = popMessageProcessor.buildCkMsg(pointWrapper.getCk(),
            pointWrapper.getReviveQueueId());
        msgInner.setDeliverTimeMs(System.currentTimeMillis() + invisibleTime);
        HookUtils.handleScheduleMessage(brokerController, msgInner);
        PutMessageResult putMessageResult = brokerController.getMessageStore().putMessage(msgInner);
        if (putMessageResult.getPutMessageStatus() != PutMessageStatus.PUT_OK
            && putMessageResult.getPutMessageStatus() != PutMessageStatus.FLUSH_DISK_TIMEOUT
            && putMessageResult.getPutMessageStatus() != PutMessageStatus.FLUSH_SLAVE_TIMEOUT
            && putMessageResult.getPutMessageStatus() != PutMessageStatus.SLAVE_NOT_AVAILABLE) {
            return false;
        }
        pointWrapper.setCkStored(true);
        pointWrapper.setReviveQueueOffset(putMessageResult.getAppendMessageResult().getLogicsOffset());
        return true;
    }

    private boolean putAckToStore(PopBufferMergeService.PopCheckPointWrapper pointWrapper, AckMsg ackMsg, int rqId,
        long invisible) {
        MessageExtBrokerInner msgInner = new MessageExtBrokerInner();
        msgInner.setTopic(popMessageProcessor.reviveTopic);
        msgInner.setBody(JSON.toJSONString(ackMsg).getBytes(DataConverter.charset));
        // msgInner.setQueueId(Integer.valueOf(extraInfo[3]));
        msgInner.setQueueId(rqId);
        msgInner.setTags(PopAckConstants.ACK_TAG);
        msgInner.setBornTimestamp(System.currentTimeMillis());
        msgInner.setBornHost(this.brokerController.getStoreHost());
        msgInner.setStoreHost(this.brokerController.getStoreHost());
        msgInner.setDeliverTimeMs(System.currentTimeMillis() + invisible);
        msgInner.getProperties().put(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX,
            PopMessageProcessor.genAckUniqueId(ackMsg));
        msgInner.setPropertiesString(MessageDecoder.messageProperties2String(msgInner.getProperties()));
        HookUtils.handleScheduleMessage(brokerController, msgInner);

        PutMessageResult putMessageResult = brokerController.getMessageStore().putMessage(msgInner);
        return putMessageResult.getPutMessageStatus() == PutMessageStatus.PUT_OK;
    }

}