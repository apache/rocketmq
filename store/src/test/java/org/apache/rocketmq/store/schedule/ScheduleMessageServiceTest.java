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

package org.apache.rocketmq.store.schedule;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.store.*;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.apache.rocketmq.store.stats.BrokerStatsManager;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;


public class ScheduleMessageServiceTest {


    /**t
     * defaultMessageDelayLevel = "1s 5s 10s 30s 1m 2m 3m 4m 5m 6m 7m 8m 9m 10m 20m 30m 1h 2h"
     */
    String testMessageDelayLevel = "5s 10s";
    /**
     * choose delay level
     * 1 = 5s
     */
    int delayLevel = 1;

    private static final String storePath = System.getProperty("user.home")  + File.separator + "schedule_test"+ UUID.randomUUID();
    private static final int commitLogFileSize = 1024;
    private static final int cqFileSize = 10;
    private static final int cqExtFileSize = 10 * (ConsumeQueueExt.CqExtUnit.MIN_EXT_UNIT_SIZE + 64);

    private static SocketAddress bornHost;
    private static SocketAddress storeHost;
    DefaultMessageStore messageStore;
    MessageStoreConfig messageStoreConfig;
    BrokerConfig brokerConfig;
    ScheduleMessageService scheduleMessageService;

    static String sendMessage =   " ------- schedule message test -------";


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
    public void init() throws Exception {
        messageStoreConfig = new MessageStoreConfig();
        messageStoreConfig.setMessageDelayLevel(testMessageDelayLevel);
        messageStoreConfig.setMapedFileSizeCommitLog(commitLogFileSize);
        messageStoreConfig.setMapedFileSizeConsumeQueue(cqFileSize);
        messageStoreConfig.setMappedFileSizeConsumeQueueExt(cqExtFileSize);
        messageStoreConfig.setMessageIndexEnable(false);
        messageStoreConfig.setEnableConsumeQueueExt(true);
        messageStoreConfig.setStorePathRootDir(storePath);
        messageStoreConfig.setStorePathCommitLog(storePath + File.separator + "commitlog");

        brokerConfig = new BrokerConfig();
        BrokerStatsManager manager = new BrokerStatsManager(brokerConfig.getBrokerClusterName());
        messageStore = new DefaultMessageStore(messageStoreConfig, manager, new MyMessageArrivingListener(), new BrokerConfig());

        assertThat(messageStore.load()).isTrue();

        messageStore.start();
        scheduleMessageService = messageStore.getScheduleMessageService();
    }



    @Test
    public void buildRunningStatsTest() throws InterruptedException {
        MessageExtBrokerInner msg = buildMessage();
        msg.setDelayTimeLevel(delayLevel);
        messageStore.putMessage(msg);
        // wait offsetTable
        TimeUnit.SECONDS.sleep(1);
        scheduleMessageService.buildRunningStats(new HashMap<String, String>() );
    }


    @Test
    public void computeDeliverTimestampTest() {
        // testMessageDelayLevel  just "5s 10s"
        long storeTime = System.currentTimeMillis();
        long time1 = scheduleMessageService.computeDeliverTimestamp(1, storeTime);
        assertThat(time1).isEqualTo(storeTime + 5 * 1000);

        long time2 = scheduleMessageService.computeDeliverTimestamp(2, storeTime);
        assertThat(time2).isEqualTo(storeTime + 10 * 1000);

    }


    @Test
    public void delayLevel2QueueIdTest() {
        int queueId = ScheduleMessageService.delayLevel2QueueId(delayLevel);
        assertThat(queueId).isEqualTo(delayLevel - 1);
        queueId = ScheduleMessageService.queueId2DelayLevel(delayLevel);
        assertThat(queueId).isEqualTo(delayLevel + 1);
    }

    @Test
    public void deliverDelayedMessageTimerTaskTest() throws InterruptedException {
        MessageExtBrokerInner msg = buildMessage();
        // set delayLevel,and send delay message
        msg.setDelayTimeLevel(delayLevel);
        PutMessageResult result = messageStore.putMessage(msg);
        assertThat(result.isOk()).isTrue();

        // consumer message
        Long offset = result.getAppendMessageResult().getLogicsOffset();
        String messageGroup = "delayGroupTest";
        GetMessageResult messageResult = messageStore.getMessage(messageGroup,msg.getTopic(),
                msg.getQueueId(),offset,1,null);

        // now, no message in queue,must wait > 5 seconds
        assertThat(messageResult.getStatus()).isEqualTo(GetMessageStatus.NO_MESSAGE_IN_QUEUE);


        TimeUnit.SECONDS.sleep(6);
        messageResult = messageStore.getMessage(messageGroup,msg.getTopic(),
                msg.getQueueId(),offset,1,null);
        // now,found the message
        assertThat(messageResult.getStatus()).isEqualTo(GetMessageStatus.FOUND);


        // get the message body
        ByteBuffer byteBuffer = ByteBuffer.allocate(messageResult.getBufferTotalSize());
        List<ByteBuffer>  byteBufferList = messageResult.getMessageBufferList();
        for (ByteBuffer bb : byteBufferList) {
            byteBuffer.put(bb);
        }

        // warp and decode the message
        byteBuffer = ByteBuffer.wrap(byteBuffer.array());
        List<MessageExt> msgList = MessageDecoder.decodes(byteBuffer);
        String retryMsg = new String(msgList.get(0).getBody());
        assertThat(sendMessage).isEqualTo(retryMsg);

        // add mapFile release
        messageResult.release();

    }

    @Test
    public void persist(){
        // because of the method will wait 10s
        scheduleMessageService.persist();
    }


    @After
    public void shutdown() throws InterruptedException {
        TimeUnit.SECONDS.sleep(1);
        scheduleMessageService.shutdown();
        messageStore.shutdown();
        messageStore.destroy();
        File file = new File(messageStoreConfig.getStorePathRootDir());
        UtilAll.deleteFile(file);
    }


    public MessageExtBrokerInner buildMessage() {

        byte[] msgBody = sendMessage.getBytes();
        MessageExtBrokerInner msg = new MessageExtBrokerInner();
        msg.setTopic("schedule_topic_test");
        msg.setTags("schedule_tag");
        msg.setKeys("schedule_key");
        msg.setBody(msgBody);
        msg.setQueueId(0);
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
