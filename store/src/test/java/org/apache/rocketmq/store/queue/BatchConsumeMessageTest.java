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

package org.apache.rocketmq.store.queue;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.attribute.CQType;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageExtBrokerInner;
import org.apache.rocketmq.common.sysflag.MessageSysFlag;
import org.apache.rocketmq.common.utils.QueueTypeUtils;
import org.apache.rocketmq.store.ConsumeQueueExt;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.GetMessageResult;
import org.apache.rocketmq.store.GetMessageStatus;
import org.apache.rocketmq.store.MessageFilter;
import org.apache.rocketmq.store.PutMessageResult;
import org.apache.rocketmq.store.PutMessageStatus;
import org.apache.rocketmq.store.SelectMappedBufferResult;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.awaitility.Awaitility.await;

public class BatchConsumeMessageTest extends QueueTestBase {
    private static final int BATCH_NUM = 10;
    private static final int TOTAL_MSGS = 200;
    private DefaultMessageStore messageStore;
    private ConcurrentMap<String, TopicConfig> topicConfigTableMap;

    @Before
    public void init() throws Exception {
        this.topicConfigTableMap = new ConcurrentHashMap<>();
        messageStore = (DefaultMessageStore) createMessageStore(null, true, this.topicConfigTableMap);
        messageStore.load();
        messageStore.start();
    }

    @After
    public void destroy() {
        messageStore.shutdown();
        messageStore.destroy();

        File file = new File(messageStore.getMessageStoreConfig().getStorePathRootDir());
        UtilAll.deleteFile(file);
    }

    @Test
    public void testSendMessagesToCqTopic() {
        String topic = UUID.randomUUID().toString();
        ConcurrentMap<String, TopicConfig>  topicConfigTable = createTopicConfigTable(topic, CQType.SimpleCQ);
        this.topicConfigTableMap.putAll(topicConfigTable);

//        int batchNum = 10;

        // case 1 has PROPERTY_INNER_NUM but has no INNER_BATCH_FLAG
//        MessageExtBrokerInner messageExtBrokerInner = buildMessage(topic, batchNum);
//        messageExtBrokerInner.setSysFlag(0);
//        PutMessageResult putMessageResult = messageStore.putMessage(messageExtBrokerInner);
//        Assert.assertEquals(PutMessageStatus.MESSAGE_ILLEGAL, putMessageResult.getPutMessageStatus());

        // case 2 has PROPERTY_INNER_NUM and has INNER_BATCH_FLAG, but is not a batchCq
//        MessageExtBrokerInner messageExtBrokerInner = buildMessage(topic, 1);
//        PutMessageResult putMessageResult = messageStore.putMessage(messageExtBrokerInner);
//        Assert.assertEquals(PutMessageStatus.MESSAGE_ILLEGAL, putMessageResult.getPutMessageStatus());

        // case 3 has neither PROPERTY_INNER_NUM nor INNER_BATCH_FLAG.
        MessageExtBrokerInner messageExtBrokerInner = buildMessage(topic, -1);
        PutMessageResult putMessageResult = messageStore.putMessage(messageExtBrokerInner);
        Assert.assertEquals(PutMessageStatus.PUT_OK, putMessageResult.getPutMessageStatus());
    }

    @Test
    public void testSendMessagesToBcqTopic() {
        String topic = UUID.randomUUID().toString();
        ConcurrentMap<String, TopicConfig>  topicConfigTable = createTopicConfigTable(topic, CQType.BatchCQ);
        this.topicConfigTableMap.putAll(topicConfigTable);

        // case 1 has PROPERTY_INNER_NUM but has no INNER_BATCH_FLAG
//        MessageExtBrokerInner messageExtBrokerInner = buildMessage(topic, 1);
//        PutMessageResult putMessageResult = messageStore.putMessage(messageExtBrokerInner);
//        Assert.assertEquals(PutMessageStatus.MESSAGE_ILLEGAL, putMessageResult.getPutMessageStatus());

        // case 2 has neither PROPERTY_INNER_NUM nor INNER_BATCH_FLAG.
        MessageExtBrokerInner messageExtBrokerInner = buildMessage(topic, -1);
        PutMessageResult putMessageResult = messageStore.putMessage(messageExtBrokerInner);
        Assert.assertEquals(PutMessageStatus.PUT_OK, putMessageResult.getPutMessageStatus());

        // case 3 has INNER_BATCH_FLAG but has no PROPERTY_INNER_NUM.
        messageExtBrokerInner = buildMessage(topic, 1);
        MessageAccessor.clearProperty(messageExtBrokerInner, MessageConst.PROPERTY_INNER_NUM);
        messageExtBrokerInner.setSysFlag(MessageSysFlag.INNER_BATCH_FLAG);
        putMessageResult = messageStore.putMessage(messageExtBrokerInner);
        Assert.assertEquals(PutMessageStatus.PUT_OK, putMessageResult.getPutMessageStatus());
    }

    @Test
    public void testConsumeBatchMessage() {
        String topic = UUID.randomUUID().toString();
        ConcurrentMap<String, TopicConfig>  topicConfigTable = createTopicConfigTable(topic, CQType.BatchCQ);
        this.topicConfigTableMap.putAll(topicConfigTable);
        int batchNum = 10;

        MessageExtBrokerInner messageExtBrokerInner = buildMessage(topic, batchNum);
        PutMessageResult putMessageResult = messageStore.putMessage(messageExtBrokerInner);
        Assert.assertEquals(PutMessageStatus.PUT_OK, putMessageResult.getPutMessageStatus());

        await().atMost(5, SECONDS).until(fullyDispatched(messageStore));

        List<GetMessageResult> results = new ArrayList<>();
        for (int i = 0; i < batchNum; i++) {
            GetMessageResult result = messageStore.getMessage("whatever", topic, 0, i, Integer.MAX_VALUE, Integer.MAX_VALUE, null);
            try {
                Assert.assertEquals(GetMessageStatus.FOUND, result.getStatus());
                results.add(result);
            } finally {
                result.release();
            }
        }

        for (GetMessageResult result : results) {
            Assert.assertEquals(0, result.getMinOffset());
            Assert.assertEquals(batchNum, result.getMaxOffset());
        }

    }

    @Test
    public void testNextBeginOffsetConsumeBatchMessage() {
        String topic = UUID.randomUUID().toString();
        ConcurrentMap<String, TopicConfig>  topicConfigTable = createTopicConfigTable(topic, CQType.BatchCQ);
        this.topicConfigTableMap.putAll(topicConfigTable);
        Random random = new Random();
        int putMessageCount = 1000;

        Queue<Integer> queue = new ArrayDeque<>();
        for (int i = 0; i < putMessageCount; i++) {
            int batchNum = random.nextInt(1000) + 2;
            MessageExtBrokerInner messageExtBrokerInner = buildMessage(topic, batchNum);
            PutMessageResult putMessageResult = messageStore.putMessage(messageExtBrokerInner);
            Assert.assertEquals(PutMessageStatus.PUT_OK, putMessageResult.getPutMessageStatus());
            queue.add(batchNum);
        }

        await().atMost(5, SECONDS).until(fullyDispatched(messageStore));

        long pullOffset = 0L;
        int getMessageCount = 0;
        int atMostMsgNum = 1;
        while (true) {
            GetMessageResult getMessageResult = messageStore.getMessage("group", topic, 0, pullOffset, atMostMsgNum, null);
            if (Objects.equals(getMessageResult.getStatus(), GetMessageStatus.OFFSET_OVERFLOW_ONE)) {
                break;
            }
            Assert.assertEquals(1, getMessageResult.getMessageQueueOffset().size());
            Long baseOffset = getMessageResult.getMessageQueueOffset().get(0);
            Integer batchNum = queue.poll();
            Assert.assertNotNull(batchNum);
            Assert.assertEquals(baseOffset + batchNum, getMessageResult.getNextBeginOffset());
            pullOffset = getMessageResult.getNextBeginOffset();
            getMessageCount++;
        }
        Assert.assertEquals(putMessageCount, getMessageCount);
    }

    @Test
    public void testGetOffsetInQueueByTime() throws Exception {
        String topic = "testGetOffsetInQueueByTime";

        ConcurrentMap<String, TopicConfig>  topicConfigTable = createTopicConfigTable(topic, CQType.BatchCQ);
        this.topicConfigTableMap.putAll(topicConfigTable);
        Assert.assertTrue(QueueTypeUtils.isBatchCq(messageStore.getTopicConfig(topic)));

        // The initial min max offset, before and after the creation of consume queue
        Assert.assertEquals(0, messageStore.getMaxOffsetInQueue(topic, 0));
        Assert.assertEquals(-1, messageStore.getMinOffsetInQueue(topic, 0));

        int batchNum = 10;
        long timeMid = -1;
        for (int i = 0; i < 19; i++) {
            PutMessageResult putMessageResult = messageStore.putMessage(buildMessage(topic, batchNum));
            Assert.assertEquals(PutMessageStatus.PUT_OK, putMessageResult.getPutMessageStatus());
            Thread.sleep(2);
            if (i == 7)
                timeMid = System.currentTimeMillis();
        }

        await().atMost(5, SECONDS).until(fullyDispatched(messageStore));

        Assert.assertEquals(80, messageStore.getOffsetInQueueByTime(topic, 0, timeMid));
        Assert.assertEquals(0, messageStore.getMinOffsetInQueue(topic, 0));
        Assert.assertEquals(190, messageStore.getMaxOffsetInQueue(topic, 0));

        int maxBatchDeleteFilesNum = messageStore.getMessageStoreConfig().getMaxBatchDeleteFilesNum();
        messageStore.getCommitLog().deleteExpiredFile(1L, 100, 12000, true, maxBatchDeleteFilesNum);
        Assert.assertEquals(80, messageStore.getOffsetInQueueByTime(topic, 0, timeMid));

        // can set periodic interval for executing  DefaultMessageStore.this.cleanFilesPeriodically() method, we can execute following code.
        // default periodic interval is 60s, This code snippet will take 60 seconds.
        /*final long a = timeMid;
        await().atMost(Duration.ofMinutes(2)).until(()->{
            long time = messageStore.getOffsetInQueueByTime(topic, 0, a);
            return 180 ==time;
        });
        Assert.assertEquals(180, messageStore.getOffsetInQueueByTime(topic, 0, timeMid));*/
    }

    @Test
    public void testDispatchNormalConsumeQueue() throws Exception {
        String topic = "TestDispatchBuildConsumeQueue";
        ConcurrentMap<String, TopicConfig>  topicConfigTable = createTopicConfigTable(topic, CQType.SimpleCQ);
        this.topicConfigTableMap.putAll(topicConfigTable);

        long timeStart = -1;
        long timeMid = -1;
        long commitLogMid = -1;

        for (int i = 0; i < 100; i++) {
            MessageExtBrokerInner messageExtBrokerInner = buildMessage(topic, -1);
            PutMessageResult putMessageResult = messageStore.putMessage(messageExtBrokerInner);
            Assert.assertEquals(PutMessageStatus.PUT_OK, putMessageResult.getPutMessageStatus());

            Thread.sleep(2);
            if (i == 0) {
                timeStart = putMessageResult.getAppendMessageResult().getStoreTimestamp();
            }
            if (i == 50) {
                timeMid = putMessageResult.getAppendMessageResult().getStoreTimestamp();
                commitLogMid = putMessageResult.getAppendMessageResult().getWroteOffset();
            }

        }

        await().atMost(5, SECONDS).until(fullyDispatched(messageStore));

        ConsumeQueueInterface consumeQueue = messageStore.getConsumeQueue(topic, 0);
        Assert.assertEquals(CQType.SimpleCQ, consumeQueue.getCQType());
        //check the consume queue
        Assert.assertEquals(0, consumeQueue.getMinOffsetInQueue());
        Assert.assertEquals(0, consumeQueue.getOffsetInQueueByTime(0));
        Assert.assertEquals(50, consumeQueue.getOffsetInQueueByTime(timeMid));
        Assert.assertEquals(100, consumeQueue.getOffsetInQueueByTime(timeMid + Integer.MAX_VALUE));
        Assert.assertEquals(100, consumeQueue.getMaxOffsetInQueue());
        //check the messagestore
        Assert.assertEquals(100, messageStore.getMessageTotalInQueue(topic, 0));
        Assert.assertEquals(consumeQueue.getMinOffsetInQueue(), messageStore.getMinOffsetInQueue(topic, 0));
        Assert.assertEquals(consumeQueue.getMaxOffsetInQueue(), messageStore.getMaxOffsetInQueue(topic, 0));
        for (int i = -100; i < 100; i += 20) {
            Assert.assertEquals(consumeQueue.getOffsetInQueueByTime(timeMid + i), messageStore.getOffsetInQueueByTime(topic, 0, timeMid + i));
        }

        //check the message time
        long earliestMessageTime = messageStore.getEarliestMessageTime(topic, 0);
        Assert.assertEquals(timeStart, earliestMessageTime);
        long messageStoreTime = messageStore.getMessageStoreTimeStamp(topic, 0, 50);
        Assert.assertEquals(timeMid, messageStoreTime);
        long commitLogOffset = messageStore.getCommitLogOffsetInQueue(topic, 0, 50);
        Assert.assertTrue(commitLogOffset >= messageStore.getMinPhyOffset());
        Assert.assertTrue(commitLogOffset <= messageStore.getMaxPhyOffset());
        Assert.assertEquals(commitLogMid, commitLogOffset);

        Assert.assertTrue(messageStore.checkInMemByConsumeOffset(topic, 0, 50, 1));
    }

    @Test
    public void testDispatchBuildBatchConsumeQueue() throws Exception {
        String topic = "testDispatchBuildBatchConsumeQueue";
        int batchNum = 10;
        long timeStart = -1;
        long timeMid = -1;

        ConcurrentMap<String, TopicConfig>  topicConfigTable = createTopicConfigTable(topic, CQType.BatchCQ);
        this.topicConfigTableMap.putAll(topicConfigTable);

        for (int i = 0; i < 100; i++) {
            PutMessageResult putMessageResult = messageStore.putMessage(buildMessage(topic, batchNum));
            Assert.assertEquals(PutMessageStatus.PUT_OK, putMessageResult.getPutMessageStatus());
            Thread.sleep(2);
            if (i == 0) {
                timeStart = putMessageResult.getAppendMessageResult().getStoreTimestamp();
            }
            if (i == 30) {
                timeMid = putMessageResult.getAppendMessageResult().getStoreTimestamp();
            }

        }

        await().atMost(5, SECONDS).until(fullyDispatched(messageStore));

        ConsumeQueueInterface consumeQueue = messageStore.getConsumeQueue(topic, 0);
        Assert.assertEquals(CQType.BatchCQ, consumeQueue.getCQType());

        Assert.assertEquals(0, consumeQueue.getMinOffsetInQueue());
        Assert.assertEquals(1000, consumeQueue.getMaxOffsetInQueue());

        //check the message store
        Assert.assertEquals(1000, messageStore.getMessageTotalInQueue(topic, 0));
        Assert.assertEquals(consumeQueue.getMinOffsetInQueue(), messageStore.getMinOffsetInQueue(topic, 0));
        Assert.assertEquals(consumeQueue.getMaxOffsetInQueue(), messageStore.getMaxOffsetInQueue(topic, 0));
        for (int i = -100; i < 100; i += 20) {
            Assert.assertEquals(consumeQueue.getOffsetInQueueByTime(timeMid + i), messageStore.getOffsetInQueueByTime(topic, 0, timeMid + i));
        }

        //check the message time
        long earliestMessageTime = messageStore.getEarliestMessageTime(topic, 0);
        Assert.assertEquals(earliestMessageTime, timeStart);
        long messageStoreTime = messageStore.getMessageStoreTimeStamp(topic, 0, 300);
        Assert.assertEquals(messageStoreTime, timeMid);
        long commitLogOffset = messageStore.getCommitLogOffsetInQueue(topic, 0, 300);
        Assert.assertTrue(commitLogOffset >= messageStore.getMinPhyOffset());
        Assert.assertTrue(commitLogOffset <= messageStore.getMaxPhyOffset());

        Assert.assertTrue(messageStore.checkInMemByConsumeOffset(topic, 0, 300, 1));

        //get the message Normally
        GetMessageResult getMessageResult = messageStore.getMessage("group", topic, 0, 0, 10 * batchNum, null);
        Assert.assertEquals(10, getMessageResult.getMessageMapedList().size());
        for (int i = 0; i < 10; i++) {
            SelectMappedBufferResult sbr = getMessageResult.getMessageMapedList().get(i);
            MessageExt messageExt = MessageDecoder.decode(sbr.getByteBuffer());
            short tmpBatchNum = Short.parseShort(messageExt.getProperty(MessageConst.PROPERTY_INNER_NUM));
            Assert.assertEquals(i * batchNum, Long.parseLong(messageExt.getProperty(MessageConst.PROPERTY_INNER_BASE)));
            Assert.assertEquals(batchNum, tmpBatchNum);
        }
    }

    @Test
    public void testGetBatchMessageWithinNumber() {
        String topic = UUID.randomUUID().toString();

        ConcurrentMap<String, TopicConfig>  topicConfigTable = createTopicConfigTable(topic, CQType.BatchCQ);
        this.topicConfigTableMap.putAll(topicConfigTable);

        int batchNum = 20;
        for (int i = 0; i < 200; i++) {
            PutMessageResult putMessageResult = messageStore.putMessage(buildMessage(topic, batchNum));
            Assert.assertEquals(PutMessageStatus.PUT_OK, putMessageResult.getPutMessageStatus());
            Assert.assertEquals(i * batchNum, putMessageResult.getAppendMessageResult().getLogicsOffset());
            Assert.assertEquals(batchNum, putMessageResult.getAppendMessageResult().getMsgNum());
        }

        await().atMost(5, SECONDS).until(fullyDispatched(messageStore));

        ConsumeQueueInterface consumeQueue = messageStore.getConsumeQueue(topic, 0);
        Assert.assertEquals(CQType.BatchCQ, consumeQueue.getCQType());
        Assert.assertEquals(0, consumeQueue.getMinOffsetInQueue());
        Assert.assertEquals(200 * batchNum, consumeQueue.getMaxOffsetInQueue());

        {
            GetMessageResult getMessageResult = messageStore.getMessage("group", topic, 0, 5, 1, Integer.MAX_VALUE, null);
            Assert.assertEquals(GetMessageStatus.FOUND, getMessageResult.getStatus());
            Assert.assertEquals(batchNum, getMessageResult.getNextBeginOffset());
            Assert.assertEquals(1, getMessageResult.getMessageMapedList().size());
            Assert.assertEquals(batchNum, getMessageResult.getMessageCount());
            SelectMappedBufferResult sbr = getMessageResult.getMessageMapedList().get(0);
            MessageExt messageExt = MessageDecoder.decode(sbr.getByteBuffer());
            short tmpBatchNum = Short.parseShort(messageExt.getProperty(MessageConst.PROPERTY_INNER_NUM));
            Assert.assertEquals(0, messageExt.getQueueOffset());
            Assert.assertEquals(batchNum, tmpBatchNum);
        }
        {
            GetMessageResult getMessageResult = messageStore.getMessage("group", topic, 0, 5, 39, Integer.MAX_VALUE, null);
            Assert.assertEquals(GetMessageStatus.FOUND, getMessageResult.getStatus());
            Assert.assertEquals(1, getMessageResult.getMessageMapedList().size());
            Assert.assertEquals(batchNum, getMessageResult.getNextBeginOffset());
            Assert.assertEquals(batchNum, getMessageResult.getMessageCount());

        }

        {
            GetMessageResult getMessageResult = messageStore.getMessage("group", topic, 0, 5, 60, Integer.MAX_VALUE, null);
            Assert.assertEquals(GetMessageStatus.FOUND, getMessageResult.getStatus());
            Assert.assertEquals(3, getMessageResult.getMessageMapedList().size());
            Assert.assertEquals(3 * batchNum, getMessageResult.getNextBeginOffset());
            Assert.assertEquals(3 * batchNum, getMessageResult.getMessageCount());
            for (int i = 0; i < getMessageResult.getMessageBufferList().size(); i++) {
                Assert.assertFalse(getMessageResult.getMessageMapedList().get(i).hasReleased());
                SelectMappedBufferResult sbr = getMessageResult.getMessageMapedList().get(i);
                MessageExt messageExt = MessageDecoder.decode(sbr.getByteBuffer());
                Assert.assertNotNull(messageExt);
                short innerBatchNum = Short.parseShort(messageExt.getProperty(MessageConst.PROPERTY_INNER_NUM));
                Assert.assertEquals(i * batchNum, Long.parseLong(messageExt.getProperty(MessageConst.PROPERTY_INNER_BASE)));
                Assert.assertEquals(batchNum, innerBatchNum);

            }
        }
    }

    @Test
    public void testGetBatchMessageWithinSize() {
        String topic = UUID.randomUUID().toString();
        ConcurrentMap<String, TopicConfig>  topicConfigTable = createTopicConfigTable(topic, CQType.BatchCQ);
        this.topicConfigTableMap.putAll(topicConfigTable);

        int batchNum = 10;
        for (int i = 0; i < 100; i++) {
            PutMessageResult putMessageResult = messageStore.putMessage(buildMessage(topic, batchNum));
            Assert.assertEquals(PutMessageStatus.PUT_OK, putMessageResult.getPutMessageStatus());
            Assert.assertEquals(i * 10, putMessageResult.getAppendMessageResult().getLogicsOffset());
            Assert.assertEquals(batchNum, putMessageResult.getAppendMessageResult().getMsgNum());
        }

        await().atMost(5, SECONDS).until(fullyDispatched(messageStore));

        ConsumeQueueInterface consumeQueue = messageStore.getConsumeQueue(topic, 0);
        Assert.assertEquals(CQType.BatchCQ, consumeQueue.getCQType());
        Assert.assertEquals(0, consumeQueue.getMinOffsetInQueue());
        Assert.assertEquals(1000, consumeQueue.getMaxOffsetInQueue());

        {
            GetMessageResult getMessageResult = messageStore.getMessage("group", topic, 0, 5, Integer.MAX_VALUE, 100, null);
            Assert.assertEquals(GetMessageStatus.FOUND, getMessageResult.getStatus());
            Assert.assertEquals(10, getMessageResult.getNextBeginOffset());
            Assert.assertEquals(1, getMessageResult.getMessageMapedList().size());
            SelectMappedBufferResult sbr = getMessageResult.getMessageMapedList().get(0);
            MessageExt messageExt = MessageDecoder.decode(sbr.getByteBuffer());
            short tmpBatchNum = Short.parseShort(messageExt.getProperty(MessageConst.PROPERTY_INNER_NUM));
            Assert.assertEquals(0, messageExt.getQueueOffset());
            Assert.assertEquals(batchNum, tmpBatchNum);
        }
        {
            GetMessageResult getMessageResult = messageStore.getMessage("group", topic, 0, 5, Integer.MAX_VALUE, 2048, null);
            Assert.assertEquals(GetMessageStatus.FOUND, getMessageResult.getStatus());
            Assert.assertEquals(1, getMessageResult.getMessageMapedList().size());
            Assert.assertEquals(10, getMessageResult.getNextBeginOffset());

        }

        {
            GetMessageResult getMessageResult = messageStore.getMessage("group", topic, 0, 5, Integer.MAX_VALUE, 4096, null);
            Assert.assertEquals(GetMessageStatus.FOUND, getMessageResult.getStatus());
            Assert.assertEquals(3, getMessageResult.getMessageMapedList().size());
            Assert.assertEquals(30, getMessageResult.getNextBeginOffset());
            for (int i = 0; i < getMessageResult.getMessageBufferList().size(); i++) {
                Assert.assertFalse(getMessageResult.getMessageMapedList().get(i).hasReleased());
                SelectMappedBufferResult sbr = getMessageResult.getMessageMapedList().get(i);
                MessageExt messageExt = MessageDecoder.decode(sbr.getByteBuffer());
                short tmpBatchNum = Short.parseShort(messageExt.getProperty(MessageConst.PROPERTY_INNER_NUM));
                Assert.assertEquals(i * batchNum, Long.parseLong(messageExt.getProperty(MessageConst.PROPERTY_INNER_BASE)));
                Assert.assertEquals(batchNum, tmpBatchNum);

            }
        }
    }

    protected void putMsg(String topic) {
        ConcurrentMap<String, TopicConfig>  topicConfigTable = createTopicConfigTable(topic, CQType.BatchCQ);
        this.topicConfigTableMap.putAll(topicConfigTable);

        for (int i = 0; i < TOTAL_MSGS; i++) {
            MessageExtBrokerInner message = buildMessage(topic, BATCH_NUM * (i % 2 + 1));
            switch (i % 3) {
                case 0:
                    message.setTags("TagA");
                    break;

                case 1:
                    message.setTags("TagB");
                    break;
            }
            message.setTagsCode(message.getTags().hashCode());
            message.setPropertiesString(MessageDecoder.messageProperties2String(message.getProperties()));
            PutMessageResult putMessageResult = messageStore.putMessage(message);
            Assert.assertEquals(PutMessageStatus.PUT_OK, putMessageResult.getPutMessageStatus());
        }

        await().atMost(5, SECONDS).until(fullyDispatched(messageStore));
    }

    @Test
    public void testEstimateMessageCountInEmptyConsumeQueue() {
        String topic = UUID.randomUUID().toString();
        ConsumeQueueInterface consumeQueue = messageStore.findConsumeQueue(topic, 0);
        MessageFilter filter = new MessageFilter() {
            @Override
            public boolean isMatchedByConsumeQueue(Long tagsCode, ConsumeQueueExt.CqExtUnit cqExtUnit) {
                return tagsCode == "TagA".hashCode();
            }

            @Override
            public boolean isMatchedByCommitLog(ByteBuffer msgBuffer, Map<String, String> properties) {
                return false;
            }
        };
        long estimation = consumeQueue.estimateMessageCount(0, 0, filter);
        Assert.assertEquals(-1, estimation);

        // test for illegal offset
        estimation = consumeQueue.estimateMessageCount(0, 100, filter);
        Assert.assertEquals(-1, estimation);
        estimation = consumeQueue.estimateMessageCount(100, 1000, filter);
        Assert.assertEquals(-1, estimation);
    }

    @Test
    public void testEstimateMessageCount() {
        String topic = UUID.randomUUID().toString();
        putMsg(topic);
        ConsumeQueueInterface cq = messageStore.findConsumeQueue(topic, 0);
        MessageFilter filter = new MessageFilter() {
            @Override
            public boolean isMatchedByConsumeQueue(Long tagsCode, ConsumeQueueExt.CqExtUnit cqExtUnit) {
                return tagsCode == "TagA".hashCode();
            }

            @Override
            public boolean isMatchedByCommitLog(ByteBuffer msgBuffer, Map<String, String> properties) {
                return false;
            }
        };
        long estimation = cq.estimateMessageCount(0, 2999, filter);
        Assert.assertEquals(1000, estimation);

        // test for illegal offset
        estimation = cq.estimateMessageCount(0, Long.MAX_VALUE, filter);
        Assert.assertEquals(-1, estimation);
        estimation = cq.estimateMessageCount(100000, 1000000, filter);
        Assert.assertEquals(-1, estimation);
        estimation = cq.estimateMessageCount(100, 0, filter);
        Assert.assertEquals(-1, estimation);
    }

    @Test
    public void testEstimateMessageCountSample() {
        String topic = UUID.randomUUID().toString();
        putMsg(topic);
        messageStore.getMessageStoreConfig().setSampleCountThreshold(10);
        messageStore.getMessageStoreConfig().setMaxConsumeQueueScan(20);
        ConsumeQueueInterface cq = messageStore.findConsumeQueue(topic, 0);
        MessageFilter filter = new MessageFilter() {
            @Override
            public boolean isMatchedByConsumeQueue(Long tagsCode, ConsumeQueueExt.CqExtUnit cqExtUnit) {
                return tagsCode == "TagA".hashCode();
            }

            @Override
            public boolean isMatchedByCommitLog(ByteBuffer msgBuffer, Map<String, String> properties) {
                return false;
            }
        };
        long estimation = cq.estimateMessageCount(1000, 2000, filter);
        Assert.assertEquals(300, estimation);
    }
}
