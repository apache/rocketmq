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

import org.apache.rocketmq.common.attribute.CQType;
import org.apache.rocketmq.store.ConsumeQueueExt;
import org.apache.rocketmq.store.DispatchRequest;
import org.apache.rocketmq.store.MessageStore;
import org.junit.Assert;
import org.junit.Test;

import java.util.UUID;

public class ConsumeQueueTest extends QueueTestBase {

    @Test
    public void testIterator() throws Exception {
        final int msgNum = 100;
        final int msgSize = 1000;
        MessageStore messageStore =  createMessageStore(null, true);
        messageStore.load();
        String topic = UUID.randomUUID().toString();
        //The initial min max offset, before and after the creation of consume queue
        Assert.assertEquals(0, messageStore.getMaxOffsetInQueue(topic, 0));
        Assert.assertEquals(0, messageStore.getMinOffsetInQueue(topic, 0));

        ConsumeQueueInterface consumeQueue = messageStore.getConsumeQueue(topic, 0);
        Assert.assertEquals(CQType.SimpleCQ, consumeQueue.getCQType());
        Assert.assertEquals(0, consumeQueue.getMaxOffsetInQueue());
        Assert.assertEquals(0, consumeQueue.getMinOffsetInQueue());
        Assert.assertEquals(0, messageStore.getMaxOffsetInQueue(topic, 0));
        Assert.assertEquals(0, messageStore.getMinOffsetInQueue(topic, 0));
        for (int i = 0; i < msgNum; i++) {
            DispatchRequest request = new DispatchRequest(consumeQueue.getTopic(), consumeQueue.getQueueId(), i * msgSize, msgSize, i,
                System.currentTimeMillis(), i, null, null, 0, 0, null);
            request.setBitMap(new byte[10]);
            messageStore.getQueueStore().putMessagePositionInfoWrapper(consumeQueue, request);
        }
        Assert.assertEquals(0, consumeQueue.getMinLogicOffset());
        Assert.assertEquals(0, consumeQueue.getMinOffsetInQueue());
        Assert.assertEquals(msgNum, consumeQueue.getMaxOffsetInQueue());
        Assert.assertEquals(msgNum, consumeQueue.getMessageTotalInQueue());
        //TO DO Should test it
        //Assert.assertEquals(100 * 100, consumeQueue.getMaxPhysicOffset());


        Assert.assertNull(consumeQueue.iterateFrom(-1));
        Assert.assertNull(consumeQueue.iterateFrom(msgNum));

        {
            CqUnit first = consumeQueue.getEarliestUnit();
            Assert.assertNotNull(first);
            Assert.assertEquals(0, first.getQueueOffset());
            Assert.assertEquals(msgSize, first.getSize());
            Assert.assertTrue(first.isTagsCodeValid());
        }
        {
            CqUnit last = consumeQueue.getLatestUnit();
            Assert.assertNotNull(last);
            Assert.assertEquals(msgNum - 1, last.getQueueOffset());
            Assert.assertEquals(msgSize, last.getSize());
            Assert.assertTrue(last.isTagsCodeValid());
        }

        for (int i = 0; i < msgNum; i++) {
            ReferredIterator<CqUnit> iterator = consumeQueue.iterateFrom(i);
            Assert.assertNotNull(iterator);
            long queueOffset = i;
            while (iterator.hasNext()) {
                CqUnit cqUnit =  iterator.next();
                Assert.assertEquals(queueOffset, cqUnit.getQueueOffset());
                Assert.assertEquals(queueOffset * msgSize, cqUnit.getPos());
                Assert.assertEquals(msgSize, cqUnit.getSize());
                Assert.assertTrue(cqUnit.isTagsCodeValid());
                Assert.assertEquals(queueOffset, cqUnit.getTagsCode());
                Assert.assertEquals(queueOffset, cqUnit.getValidTagsCodeAsLong().longValue());
                Assert.assertEquals(1, cqUnit.getBatchNum());
                Assert.assertNotNull(cqUnit.getCqExtUnit());
                ConsumeQueueExt.CqExtUnit cqExtUnit =  cqUnit.getCqExtUnit();
                Assert.assertEquals(queueOffset, cqExtUnit.getTagsCode());
                Assert.assertArrayEquals(new byte[10], cqExtUnit.getFilterBitMap());
                queueOffset++;
            }
            Assert.assertEquals(msgNum, queueOffset);
        }
        messageStore.getQueueStore().destroy(consumeQueue);
    }
}
