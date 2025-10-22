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
package org.apache.rocketmq.broker.pop;

import org.apache.rocketmq.common.constant.ConsumeInitMode;
import org.apache.rocketmq.store.GetMessageResult;
import org.apache.rocketmq.store.GetMessageStatus;
import org.apache.rocketmq.store.SelectMappedBufferResult;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

public class PopConsumerContextTest {

    @Test
    public void consumerContextTest() {
        long popTime = System.currentTimeMillis();
        PopConsumerContext context = new PopConsumerContext("127.0.0.1:6789",
            popTime, 20_000, "GroupId", true, ConsumeInitMode.MIN, "attemptId");

        Assert.assertFalse(context.isFound());
        Assert.assertEquals("127.0.0.1:6789", context.getClientHost());
        Assert.assertEquals(popTime, context.getPopTime());
        Assert.assertEquals(20_000, context.getInvisibleTime());
        Assert.assertEquals("GroupId", context.getGroupId());
        Assert.assertTrue(context.isFifo());
        Assert.assertEquals("attemptId", context.getAttemptId());
        Assert.assertEquals(0, context.getRestCount());

        GetMessageResult getMessageResult = new GetMessageResult();
        getMessageResult.setStatus(GetMessageStatus.FOUND);
        getMessageResult.setMinOffset(10L);
        getMessageResult.setMaxOffset(20L);
        getMessageResult.setNextBeginOffset(15L);
        getMessageResult.addMessage(Mockito.mock(SelectMappedBufferResult.class), 10);
        getMessageResult.addMessage(Mockito.mock(SelectMappedBufferResult.class), 12);
        getMessageResult.addMessage(Mockito.mock(SelectMappedBufferResult.class), 13);

        context.addGetMessageResult(getMessageResult,
            "TopicId", 3, PopConsumerRecord.RetryType.NORMAL_TOPIC, 1);

        Assert.assertEquals(3, context.getMessageCount());
        Assert.assertEquals(
            getMessageResult.getMaxOffset() - getMessageResult.getNextBeginOffset(), context.getRestCount());

        // check header
        Assert.assertNotNull(context.toString());
        Assert.assertEquals("0 3 1", context.getStartOffsetInfo());
        Assert.assertEquals("0 3 10,12,13", context.getMsgOffsetInfo());
        Assert.assertNotNull(context.getOrderCountInfoBuilder());
        Assert.assertEquals("", context.getOrderCountInfo());

        Assert.assertEquals(1, context.getGetMessageResultList().size());
        Assert.assertEquals(3, context.getPopConsumerRecordList().size());
    }
}
