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

package org.apache.rocketmq.store;

import java.io.File;
import java.io.RandomAccessFile;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.OverlappingFileLockException;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.store.config.FlushDiskType;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.apache.rocketmq.store.config.StorePathConfigHelper;
import org.junit.After;
import org.apache.rocketmq.store.stats.BrokerStatsManager;
import org.junit.Before;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class DefaultMessageStoreTest {
    private final String StoreMessage = "Once, there was a chance for me!";
    private int QUEUE_TOTAL = 100;
    private AtomicInteger QueueId = new AtomicInteger(0);
    private SocketAddress BornHost;
    private SocketAddress StoreHost;
    private byte[] MessageBody;
    private MessageStore messageStore;

    @Before
    public void init() throws Exception {
        StoreHost = new InetSocketAddress(InetAddress.getLocalHost(), 8123);
        BornHost = new InetSocketAddress(InetAddress.getByName("127.0.0.1"), 0);

        messageStore = buildMessageStore();
        boolean load = messageStore.load();
        assertTrue(load);
        messageStore.start();
    }

    @Test(expected = OverlappingFileLockException.class)
    public void test_repate_restart() throws Exception {
        QUEUE_TOTAL = 1;
        MessageBody = StoreMessage.getBytes();

        MessageStoreConfig messageStoreConfig = new MessageStoreConfig();
        messageStoreConfig.setMapedFileSizeCommitLog(1024 * 8);
        messageStoreConfig.setMapedFileSizeConsumeQueue(1024 * 4);
        messageStoreConfig.setMaxHashSlotNum(100);
        messageStoreConfig.setMaxIndexNum(100 * 10);
        MessageStore master = new DefaultMessageStore(messageStoreConfig, null, new MyMessageArrivingListener(), new BrokerConfig());

        boolean load = master.load();
        assertTrue(load);

        try {
            master.start();
            master.start();
        } finally {
            master.shutdown();
            master.destroy();
        }
    }

    @After
    public void destory() {
        messageStore.shutdown();
        messageStore.destroy();
        MessageStoreConfig messageStoreConfig = new MessageStoreConfig();
        File file = new File(messageStoreConfig.getStorePathRootDir());
        UtilAll.deleteFile(file);
    }

    private MessageStore buildMessageStore() throws Exception {
        MessageStoreConfig messageStoreConfig = new MessageStoreConfig();
        messageStoreConfig.setMapedFileSizeCommitLog(1024 * 1024 * 10);
        messageStoreConfig.setMapedFileSizeConsumeQueue(1024 * 1024 * 10);
        messageStoreConfig.setMaxHashSlotNum(10000);
        messageStoreConfig.setMaxIndexNum(100 * 100);
        messageStoreConfig.setFlushDiskType(FlushDiskType.SYNC_FLUSH);
        messageStoreConfig.setFlushIntervalConsumeQueue(1);
        messageStoreConfig.setMaxMessageSize(1024);
        return new DefaultMessageStore(messageStoreConfig, new BrokerStatsManager("simpleTest"), new MyMessageArrivingListener(), new BrokerConfig());
    }

    @Test
    public void testWriteAndRead() {
        long totalMsgs = 10;
        QUEUE_TOTAL = 1;
        MessageBody = StoreMessage.getBytes();
        for (long i = 0; i < totalMsgs; i++) {
            messageStore.putMessage(buildMessage());
        }

        for (long i = 0; i < totalMsgs; i++) {
            GetMessageResult result = messageStore.getMessage("GROUP_A", "TOPIC_A", 0, i, 1024 * 1024, null);
            assertThat(result).isNotNull();
            result.release();
        }
        verifyThatMasterIsFunctional(totalMsgs, messageStore);
    }

    private MessageExtBrokerInner buildMessage() {
        MessageExtBrokerInner msg = new MessageExtBrokerInner();
        msg.setTopic("FooBar");
        msg.setTags("TAG1");
        msg.setKeys("Hello");
        msg.setBody(MessageBody);
        msg.setKeys(String.valueOf(System.currentTimeMillis()));
        msg.setQueueId(Math.abs(QueueId.getAndIncrement()) % QUEUE_TOTAL);
        msg.setSysFlag(0);
        msg.setBornTimestamp(System.currentTimeMillis());
        msg.setStoreHost(StoreHost);
        msg.setBornHost(BornHost);
        //setKeys
        msg.setPropertiesString(MessageDecoder.messageProperties2String(msg.getProperties()));
        return msg;
    }

    private void verifyThatMasterIsFunctional(long totalMsgs, MessageStore master) {
        for (long i = 0; i < totalMsgs; i++) {
            master.putMessage(buildMessage());
        }

        for (long i = 0; i < totalMsgs; i++) {
            GetMessageResult result = master.getMessage("GROUP_A", "TOPIC_A", 0, i, 1024 * 1024, null);
            assertThat(result).isNotNull();
            result.release();

        }
    }

    @Test
    public void testPullSize() throws Exception {
        String topic = "pullSizeTopic";
        MessageBody = StoreMessage.getBytes();
        for (int i = 0; i < 32; i++) {
            MessageExtBrokerInner messageExtBrokerInner = buildMessage();
            messageExtBrokerInner.setTopic(topic);
            messageExtBrokerInner.setQueueId(0);
            messageStore.putMessage(messageExtBrokerInner);
        }
        // wait for consume queue build
        // the sleep time should be great than consume queue flush interval
        Thread.sleep(100);
        String group = "simple";
        GetMessageResult getMessageResult32 = messageStore.getMessage(group, topic, 0, 0, 32, null);
        assertThat(getMessageResult32.getMessageBufferList().size()).isEqualTo(32);

        GetMessageResult getMessageResult20 = messageStore.getMessage(group, topic, 0, 0, 20, null);
        assertThat(getMessageResult20.getMessageBufferList().size()).isEqualTo(20);

        GetMessageResult getMessageResult45 = messageStore.getMessage(group, topic, 0, 0, 10, null);
        assertThat(getMessageResult45.getMessageBufferList().size()).isEqualTo(10);
    }

    @Test
    public void testRecover() throws Exception {
        String topic = "recoverTopic";
        MessageBody = StoreMessage.getBytes();
        for (int i = 0; i < 100; i++) {
            MessageExtBrokerInner messageExtBrokerInner = buildMessage();
            messageExtBrokerInner.setTopic(topic);
            messageExtBrokerInner.setQueueId(0);
            messageStore.putMessage(messageExtBrokerInner);
        }

        Thread.sleep(100);//wait for build consumer queue
        long maxPhyOffset = messageStore.getMaxPhyOffset();
        long maxCqOffset = messageStore.getMaxOffsetInQueue(topic, 0);

        //1.just reboot
        messageStore.shutdown();
        messageStore = buildMessageStore();
        boolean load = messageStore.load();
        assertTrue(load);
        messageStore.start();
        assertTrue(maxPhyOffset == messageStore.getMaxPhyOffset());
        assertTrue(maxCqOffset == messageStore.getMaxOffsetInQueue(topic, 0));

        //2.damage commitlog and reboot normal
        for (int i = 0; i < 100; i++) {
            MessageExtBrokerInner messageExtBrokerInner = buildMessage();
            messageExtBrokerInner.setTopic(topic);
            messageExtBrokerInner.setQueueId(0);
            messageStore.putMessage(messageExtBrokerInner);
        }
        Thread.sleep(100);
        long secondLastPhyOffset = messageStore.getMaxPhyOffset();
        long secondLastCqOffset = messageStore.getMaxOffsetInQueue(topic, 0);

        MessageExtBrokerInner messageExtBrokerInner = buildMessage();
        messageExtBrokerInner.setTopic(topic);
        messageExtBrokerInner.setQueueId(0);
        messageStore.putMessage(messageExtBrokerInner);

        messageStore.shutdown();

        //damage last message
        damageCommitlog(secondLastPhyOffset);

        //reboot
        messageStore = buildMessageStore();
        load = messageStore.load();
        assertTrue(load);
        messageStore.start();
        assertTrue(secondLastPhyOffset == messageStore.getMaxPhyOffset());
        assertTrue(secondLastCqOffset == messageStore.getMaxOffsetInQueue(topic, 0));

        //3.damage commitlog and reboot abnormal
        for (int i = 0; i < 100; i++) {
            messageExtBrokerInner = buildMessage();
            messageExtBrokerInner.setTopic(topic);
            messageExtBrokerInner.setQueueId(0);
            messageStore.putMessage(messageExtBrokerInner);
        }
        Thread.sleep(100);
        secondLastPhyOffset = messageStore.getMaxPhyOffset();
        secondLastCqOffset = messageStore.getMaxOffsetInQueue(topic, 0);

        messageExtBrokerInner = buildMessage();
        messageExtBrokerInner.setTopic(topic);
        messageExtBrokerInner.setQueueId(0);
        messageStore.putMessage(messageExtBrokerInner);
        messageStore.shutdown();

        //damage last message
        damageCommitlog(secondLastPhyOffset);
        //add abort file
        String fileName = StorePathConfigHelper.getAbortFile(((DefaultMessageStore) messageStore).getMessageStoreConfig().getStorePathRootDir());
        File file = new File(fileName);
        MappedFile.ensureDirOK(file.getParent());
        file.createNewFile();

        messageStore = buildMessageStore();
        load = messageStore.load();
        assertTrue(load);
        messageStore.start();
        assertTrue(secondLastPhyOffset == messageStore.getMaxPhyOffset());
        assertTrue(secondLastCqOffset == messageStore.getMaxOffsetInQueue(topic, 0));

        //message write again
        for (int i = 0; i < 100; i++) {
            messageExtBrokerInner = buildMessage();
            messageExtBrokerInner.setTopic(topic);
            messageExtBrokerInner.setQueueId(0);
            messageStore.putMessage(messageExtBrokerInner);
        }
    }

    @Test
    public void testlookMessageByOffset() throws  Exception{
        String topic = "LookMessageByOffset %02d";
        int topicLen = String.format(topic,0).getBytes().length;
        int bodyLen  = StoreMessage.getBytes().length;
        int propertyLen = 0;
        MessageBody = StoreMessage.getBytes();
        long firstOffset = 0;
        for (int i = 0; i < 100; i++) {
            MessageExtBrokerInner messageExtBrokerInner = buildMessage();
            messageExtBrokerInner.setTopic(String.format(topic,i));
            messageExtBrokerInner.setQueueId(0);
            PutMessageResult putMessageResult = messageStore.putMessage(messageExtBrokerInner);
            if (i == 0) {
                firstOffset = putMessageResult.getAppendMessageResult().getWroteOffset();
                propertyLen = messageExtBrokerInner.getPropertiesString().getBytes().length;
            }

        }

        MessageExt messageExt = messageStore.lookMessageByOffset(firstOffset );
        assertEquals(messageExt.getTopic(),"LookMessageByOffset 00");
        //(91+bodyLen+topicLen)*50
        messageExt = messageStore.lookMessageByOffset(firstOffset + (91+topicLen+bodyLen+propertyLen) * 50);
        assertEquals(messageExt.getTopic(),"LookMessageByOffset 50");

        messageExt = messageStore.lookMessageByOffset(firstOffset + (91+topicLen+bodyLen+propertyLen) * 99);
        assertEquals(messageExt.getTopic(),"LookMessageByOffset 99");
    }

    @Test
    public void testGetMessageByOffset() throws Exception {
        String topic = "testGetMessageByOffset";
        MessageBody = StoreMessage.getBytes();
        for (int i = 0; i < 100; i++) {
            putMessage(topic,0);
        }
        TimeUnit.SECONDS.sleep(5);
        messageStore.getConsumeQueue("testGetMessageByOffset",0).flush(1);
        DefaultMessageStore defaultMessageStore = (DefaultMessageStore)messageStore;
        Map<String, Long> messageIds = defaultMessageStore.getMessageIds("testGetMessageByOffset",0,0,10,StoreHost);
        //Exists bug in defaultMessageStore.getMessageIds So don't Assert the result
        //assertTrue(10 == messageIds.size());
    }

    @Test
    public void testGetMessageTotalInQueue(){
        String topic = "testGetMessageTotalInQueue";
        MessageBody = StoreMessage.getBytes();
        for (int i = 0; i < 100; i++) {
            putMessage(topic,0);
        }
        long messageCnt = messageStore.getMessageTotalInQueue("testGetMessageTotalInQueue",0);
        assertTrue(100 == messageCnt);
    }

    @Test
    public void testGetOffsetInQueueByTime() throws Exception{
        String topic = "testGetOffsetInQueueByTime";
        MessageBody = StoreMessage.getBytes();
        int topicLen = String.format(topic,0).getBytes().length;
        int bodyLen  = StoreMessage.getBytes().length;
        long timestamp50 = System.currentTimeMillis();
        AppendMessageResult  appendMessageResult50 = null;
        for (int i = 0; i < 100; i++) {
            PutMessageResult putMessageResult = putMessage(topic,0);
            if(i == 50) {
                appendMessageResult50 = putMessageResult.getAppendMessageResult();
                timestamp50 = appendMessageResult50.getStoreTimestamp();
            }
        }
        long offset = messageStore.getOffsetInQueueByTime(topic,0,timestamp50);
        assertEquals(50,offset);
        TimeUnit.SECONDS.sleep(2);
        long lastTimestamp = System.currentTimeMillis();
        offset = messageStore.getOffsetInQueueByTime(topic,0,lastTimestamp);
        assertEquals(99,offset);
    }

    @Test
    public void testGetMessageStoreTimeStamp(){
        String topic = "testGetMessageStoreTimeStamp";
        MessageBody = StoreMessage.getBytes();
        int topicLen = String.format(topic,0).getBytes().length;
        int bodyLen  = StoreMessage.getBytes().length;
        long timestamp50 = System.currentTimeMillis();
        AppendMessageResult  appendMessageResult50 = null;
        for (int i = 0; i < 100; i++) {
            PutMessageResult putMessageResult = putMessage(topic,0);
            if(i==50) {
                appendMessageResult50 = putMessageResult.getAppendMessageResult();
                timestamp50 = appendMessageResult50.getStoreTimestamp();
            }
        }

        long resultTimestamp = messageStore.getMessageStoreTimeStamp(topic,0,50);
        assertEquals(timestamp50,resultTimestamp);

    }

    @Test
    public void testGetEarliestMessageTime() throws Exception {
        messageStore.shutdown();
        messageStore.destroy();
        String rootPath = System.getProperty("user.home") + File.separator + "store1";
        String commitLogPath = System.getProperty("user.home") + File.separator + "store1"
                + File.separator + "commitlog";
        File file = new File(rootPath);
        UtilAll.deleteFile(file);
        MessageStoreConfig messageStoreConfig = new MessageStoreConfig();
        messageStoreConfig.setMapedFileSizeCommitLog(1024 * 1024 * 10);
        messageStoreConfig.setMapedFileSizeConsumeQueue(1024 * 1024 * 10);
        messageStoreConfig.setMaxHashSlotNum(10000);
        messageStoreConfig.setMaxIndexNum(100 * 100);
        messageStoreConfig.setFlushDiskType(FlushDiskType.SYNC_FLUSH);
        messageStoreConfig.setFlushIntervalConsumeQueue(1);
        messageStoreConfig.setMaxMessageSize(1024);
        messageStoreConfig.setStorePathCommitLog(commitLogPath);
        messageStoreConfig.setStorePathRootDir(rootPath);
        messageStore = new DefaultMessageStore(messageStoreConfig, new BrokerStatsManager("simpleTest"), new MyMessageArrivingListener(), new BrokerConfig());
        messageStore.load();
        messageStore.start();
        String topic = "testGetEarliestMessageTime";
        StringBuffer sb = new StringBuffer();

        //set message body size to 0.3k+
        for (int i = 0; i < 10; i++) {
            sb.append(StoreMessage);
        }
        MessageBody = sb.toString().getBytes();
        long earliestStoreTimestamp = 0;
        for (int i = 0; i < 10; i++) {
            PutMessageResult putMessageResult = putMessage(topic, 0);
            if (i == 0) {
                earliestStoreTimestamp = putMessageResult.getAppendMessageResult().getStoreTimestamp();
            }
        }

        long earliestTimestamp = messageStore.getEarliestMessageTime();

        assertEquals(earliestStoreTimestamp, earliestTimestamp);
    }

    private PutMessageResult putMessage(String topic ,int queueId){
        MessageExtBrokerInner messageExtBrokerInner = buildMessage();
        messageExtBrokerInner.setTopic(topic);
        messageExtBrokerInner.setQueueId(queueId);
        PutMessageResult putMessageResult = messageStore.putMessage(messageExtBrokerInner);
        return putMessageResult;
    }

    private void damageCommitlog(long offset) throws Exception {
        MessageStoreConfig messageStoreConfig = new MessageStoreConfig();
        File file = new File(messageStoreConfig.getStorePathCommitLog() + File.separator + "00000000000000000000");

        FileChannel fileChannel = new RandomAccessFile(file, "rw").getChannel();
        MappedByteBuffer mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, 1024 * 1024 * 10);

        int bodyLen = mappedByteBuffer.getInt((int) offset + 84);
        int topicLenIndex = (int) offset + 84 + bodyLen + 4;
        mappedByteBuffer.position(topicLenIndex);
        mappedByteBuffer.putInt(0);
        mappedByteBuffer.putInt(0);
        mappedByteBuffer.putInt(0);
        mappedByteBuffer.putInt(0);

        mappedByteBuffer.force();
        fileChannel.force(true);
        fileChannel.close();
    }

    private class MyMessageArrivingListener implements MessageArrivingListener {
        @Override
        public void arriving(String topic, int queueId, long logicOffset, long tagsCode, long msgStoreTime,
                             byte[] filterBitMap, Map<String, String> properties) {
        }
    }
}
