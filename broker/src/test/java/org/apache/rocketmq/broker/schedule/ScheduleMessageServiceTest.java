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

package org.apache.rocketmq.broker.schedule;

import java.io.File;
import java.lang.reflect.Field;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.failover.EscapeBridge;
import org.apache.rocketmq.broker.util.HookUtils;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageExtBrokerInner;
import org.apache.rocketmq.store.ConsumeQueueExt;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.GetMessageResult;
import org.apache.rocketmq.store.GetMessageStatus;
import org.apache.rocketmq.store.MessageArrivingListener;
import org.apache.rocketmq.store.PutMessageResult;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.apache.rocketmq.store.stats.BrokerStatsManager;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import static org.apache.rocketmq.common.stats.Stats.BROKER_PUT_NUMS;
import static org.apache.rocketmq.common.stats.Stats.TOPIC_PUT_NUMS;
import static org.apache.rocketmq.common.stats.Stats.TOPIC_PUT_SIZE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.*;

public class ScheduleMessageServiceTest {

    private BrokerController brokerController;
    private ScheduleMessageService scheduleMessageService;

    /**
     * t defaultMessageDelayLevel = "1s 5s 10s 30s 1m 2m 3m 4m 5m 6m 7m 8m 9m 10m 20m 30m 1h 2h"
     */
    String testMessageDelayLevel = "5s 8s";
    /**
     * choose delay level
     */
    int delayLevel = 3;

    private static final String storePath = System.getProperty("java.io.tmpdir") + File.separator + "schedule_test#" + UUID.randomUUID();
    private static final int commitLogFileSize = 1024;
    private static final int cqFileSize = 10;
    private static final int cqExtFileSize = 10 * (ConsumeQueueExt.CqExtUnit.MIN_EXT_UNIT_SIZE + 64);

    private static SocketAddress bornHost;
    private static SocketAddress storeHost;
    private DefaultMessageStore messageStore;
    private MessageStoreConfig messageStoreConfig;
    private BrokerConfig brokerConfig;

    static String sendMessage = " ------- schedule message test -------";
    static String topic = "schedule_topic_test";
    static String messageGroup = "delayGroupTest";
    private Random random = new Random();

    static {
        try {
            bornHost = new InetSocketAddress(InetAddress.getLocalHost(), 8123);
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
        try {
            storeHost = new InetSocketAddress(InetAddress.getByName("127.0.0.1"), 0);
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
    }

    @Before
    public void setUp() throws Exception {
        messageStoreConfig = new MessageStoreConfig();
        messageStoreConfig.setMessageDelayLevel(testMessageDelayLevel);
        messageStoreConfig.setMappedFileSizeCommitLog(commitLogFileSize);
        messageStoreConfig.setMappedFileSizeConsumeQueue(cqFileSize);
        messageStoreConfig.setMappedFileSizeConsumeQueueExt(cqExtFileSize);
        messageStoreConfig.setMessageIndexEnable(false);
        messageStoreConfig.setEnableConsumeQueueExt(true);
        messageStoreConfig.setStorePathRootDir(storePath);
        messageStoreConfig.setStorePathCommitLog(storePath + File.separator + "commitlog");
        // Let OS pick an available port
        messageStoreConfig.setHaListenPort(0);

        brokerConfig = new BrokerConfig();
        BrokerStatsManager manager = new BrokerStatsManager(brokerConfig.getBrokerClusterName(), brokerConfig.isEnableDetailStat());
        messageStore = new DefaultMessageStore(messageStoreConfig, manager, new MyMessageArrivingListener(), new BrokerConfig());

        assertThat(messageStore.load()).isTrue();

        messageStore.start();
        brokerController = Mockito.mock(BrokerController.class);
        Mockito.when(brokerController.getMessageStore()).thenReturn(messageStore);
        Mockito.when(brokerController.getMessageStoreConfig()).thenReturn(messageStoreConfig);
        Mockito.when(brokerController.getBrokerConfig()).thenReturn(brokerConfig);
        Mockito.when(brokerController.peekMasterBroker()).thenReturn(brokerController);
        Mockito.when(brokerController.getBrokerStatsManager()).thenReturn(manager);
        EscapeBridge escapeBridge = new EscapeBridge(brokerController);
        Mockito.when(brokerController.getEscapeBridge()).thenReturn(escapeBridge);
        scheduleMessageService = new ScheduleMessageService(brokerController);
        scheduleMessageService.load();
        scheduleMessageService.start();
        Mockito.when(brokerController.getScheduleMessageService()).thenReturn(scheduleMessageService);
    }

    @Test
    public void testLoad() {
        ConcurrentMap<Integer, Long> offsetTable = scheduleMessageService.getOffsetTable();
        //offsetTable.put(0, 1L);
        offsetTable.put(1, 3L);
        offsetTable.put(2, 5L);
        scheduleMessageService.persist();

        ScheduleMessageService controlInstance = new ScheduleMessageService(brokerController);
        assertTrue(controlInstance.load());

        ConcurrentMap<Integer, Long> loaded = controlInstance.getOffsetTable();
        for (long offset : loaded.values()) {
            assertEquals(0, offset);
        }
    }

    @Test
    public void testCorrectDelayOffset_whenInit() throws Exception {

        ConcurrentMap<Integer /* level */, Long/* offset */> offsetTable = null;

        scheduleMessageService = new ScheduleMessageService(brokerController);
        scheduleMessageService.parseDelayLevel();

        ConcurrentMap<Integer /* level */, Long/* offset */> offsetTable1 = new ConcurrentHashMap<>();
        for (int i = 1; i <= 2; i++) {
            offsetTable1.put(i, random.nextLong());
        }

        Field field = scheduleMessageService.getClass().getDeclaredField("offsetTable");
        field.setAccessible(true);
        field.set(scheduleMessageService, offsetTable1);

        String jsonStr = scheduleMessageService.encode();
        scheduleMessageService.decode(jsonStr);

        offsetTable = (ConcurrentMap<Integer, Long>) field.get(scheduleMessageService);

        for (Map.Entry<Integer, Long> entry : offsetTable.entrySet()) {
            assertEquals(entry.getValue(), offsetTable1.get(entry.getKey()));
        }

        boolean success = scheduleMessageService.correctDelayOffset();

        System.out.printf("correctDelayOffset %s", success);

        offsetTable = (ConcurrentMap<Integer, Long>) field.get(scheduleMessageService);

        for (long offset : offsetTable.values()) {
            assertEquals(0, offset);
        }
    }

    @Test
    public void testDeliverDelayedMessageTimerTask() throws Exception {
        assertThat(messageStore.getMessageStoreConfig().isEnableScheduleMessageStats()).isTrue();

        assertThat(messageStore.getBrokerStatsManager().getStatsItem(TOPIC_PUT_NUMS, topic)).isNull();

        MessageExtBrokerInner msg = buildMessage();
        int realQueueId = msg.getQueueId();
        // set delayLevel,and send delay message
        msg.setDelayTimeLevel(delayLevel);
        HookUtils.handleScheduleMessage(brokerController, msg);
        PutMessageResult result = messageStore.putMessage(msg);
        assertThat(result.isOk()).isTrue();

        // consumer message
        int delayQueueId = ScheduleMessageService.delayLevel2QueueId(delayLevel);
        assertThat(delayQueueId).isEqualTo(delayLevel - 1);

        Long offset = result.getAppendMessageResult().getLogicsOffset();

        // now, no message in queue,must wait > delayTime
        GetMessageResult messageResult = getMessage(realQueueId, offset);
        assertThat(messageResult.getStatus()).isEqualTo(GetMessageStatus.NO_MESSAGE_IN_QUEUE);

        // timer run maybe delay, then consumer message again
        // and wait offsetTable
        TimeUnit.SECONDS.sleep(15);
        scheduleMessageService.buildRunningStats(new HashMap<String, String>());

        messageResult = getMessage(realQueueId, offset);
        // now,found the message
        assertThat(messageResult.getStatus()).isEqualTo(GetMessageStatus.FOUND);

        // get the stats change
        assertThat(messageStore.getBrokerStatsManager().getStatsItem(BROKER_PUT_NUMS, brokerConfig.getBrokerClusterName()).getValue().sum()).isEqualTo(1);
        assertThat(messageStore.getBrokerStatsManager().getStatsItem(TOPIC_PUT_NUMS, topic).getValue().sum()).isEqualTo(1L);
        assertThat(messageStore.getBrokerStatsManager().getStatsItem(TOPIC_PUT_SIZE, topic).getValue().sum()).isEqualTo(messageResult.getBufferTotalSize());

        // get the message body
        ByteBuffer byteBuffer = ByteBuffer.allocate(messageResult.getBufferTotalSize());
        List<ByteBuffer> byteBufferList = messageResult.getMessageBufferList();
        for (ByteBuffer bb : byteBufferList) {
            byteBuffer.put(bb);
        }

        // warp and decode the message
        byteBuffer = ByteBuffer.wrap(byteBuffer.array());
        List<MessageExt> msgList = MessageDecoder.decodes(byteBuffer);
        String retryMsg = new String(msgList.get(0).getBody());
        assertThat(sendMessage).isEqualTo(retryMsg);

        //  method will wait 10s,so I run it by myself
        scheduleMessageService.persist();

        // add mapFile release
        messageResult.release();

    }

    /**
     * add some [error/no use] code test
     */
    @Test
    public void otherTest() {
        // the method no use ,why need ?
        int queueId = ScheduleMessageService.queueId2DelayLevel(delayLevel);
        assertThat(queueId).isEqualTo(delayLevel + 1);

        // error delayLevelTest
        Long time = scheduleMessageService.computeDeliverTimestamp(999, 0);
        assertThat(time).isEqualTo(1000);

        // just decode
        scheduleMessageService.decode(new DelayOffsetSerializeWrapper().toJson());
    }

    private GetMessageResult getMessage(int queueId, Long offset) {
        return messageStore.getMessage(messageGroup, topic,
            queueId, offset, 1, null);

    }

    @After
    public void shutdown() throws InterruptedException {
        scheduleMessageService.shutdown();
        messageStore.shutdown();
        messageStore.destroy();
        File file = new File(messageStoreConfig.getStorePathRootDir());
        UtilAll.deleteFile(file);
    }

    public MessageExtBrokerInner buildMessage() {

        byte[] msgBody = sendMessage.getBytes();
        MessageExtBrokerInner msg = new MessageExtBrokerInner();
        msg.setTopic(topic);
        msg.setTags("schedule_tag");
        msg.setKeys("schedule_key");
        msg.setBody(msgBody);
        msg.setSysFlag(0);
        msg.setBornTimestamp(System.currentTimeMillis());
        msg.setStoreHost(storeHost);
        msg.setBornHost(bornHost);
        return msg;
    }

    private class MyMessageArrivingListener implements MessageArrivingListener {
        @Override
        public void arriving(String topic, int queueId, long logicOffset, long tagsCode, long msgStoreTime,
            byte[] filterBitMap, Map<String, String> properties) {
        }
    }
}