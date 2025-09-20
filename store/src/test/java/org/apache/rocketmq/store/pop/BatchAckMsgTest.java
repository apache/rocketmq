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

package org.apache.rocketmq.store.pop;

import com.alibaba.fastjson2.JSON;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class BatchAckMsgTest {

    @Test
    public void testSerializeAndDeSerialize() {
        String longString = "{\"ackOffsetList\":[100, 101],\"consumerGroup\":\"group\"," +
                "\"popTime\":1679454922000,\"queueId\":3,\"startOffset\":200,\"topic\":\"topic\"}";

        BatchAckMsg batchAckMsg = new BatchAckMsg();
        List<Long> aol = new ArrayList<>(32);
        aol.add(100L);
        aol.add(101L);

        batchAckMsg.setAckOffsetList(aol);
        batchAckMsg.setStartOffset(200L);
        batchAckMsg.setConsumerGroup("group");
        batchAckMsg.setTopic("topic");
        batchAckMsg.setQueueId(3);
        batchAckMsg.setPopTime(1679454922000L);

        String jsonString = JSON.toJSONString(batchAckMsg);
        BatchAckMsg batchAckMsg1 = JSON.parseObject(jsonString, BatchAckMsg.class);
        BatchAckMsg batchAckMsg2 = JSON.parseObject(longString, BatchAckMsg.class);

        Assert.assertEquals(batchAckMsg1.getAckOffsetList(), batchAckMsg2.getAckOffsetList());
        Assert.assertEquals(batchAckMsg1.getTopic(), batchAckMsg2.getTopic());
        Assert.assertEquals(batchAckMsg1.getConsumerGroup(), batchAckMsg2.getConsumerGroup());
        Assert.assertEquals(batchAckMsg1.getQueueId(), batchAckMsg2.getQueueId());
        Assert.assertEquals(batchAckMsg1.getStartOffset(), batchAckMsg2.getStartOffset());
        Assert.assertEquals(batchAckMsg1.getPopTime(), batchAckMsg2.getPopTime());
    }
}
