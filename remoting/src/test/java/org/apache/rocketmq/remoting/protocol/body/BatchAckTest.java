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
package org.apache.rocketmq.remoting.protocol.body;

import com.alibaba.fastjson.JSON;
import org.apache.rocketmq.common.MixAll;
import org.junit.Test;

import java.util.Arrays;
import java.util.BitSet;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class BatchAckTest {
    private static String topic = "myTopic";
    private static String cid = MixAll.DEFAULT_CONSUMER_GROUP;
    private static long startOffset = 100;
    private static int qId = 1;
    private static int rqId = 2;
    private static long popTime = System.currentTimeMillis();
    private static long invisibleTime = 5000;

    @Test
    public void testBatchAckSerializerDeserializer() {
        List<Long> ackOffsetList = Arrays.asList(startOffset + 1, startOffset + 3, startOffset + 5);
        BatchAck batchAck = new BatchAck();
        batchAck.setConsumerGroup(cid);
        batchAck.setTopic(topic);
        batchAck.setRetry("0");
        batchAck.setStartOffset(startOffset);
        batchAck.setQueueId(qId);
        batchAck.setReviveQueueId(rqId);
        batchAck.setPopTime(popTime);
        batchAck.setInvisibleTime(invisibleTime);
        batchAck.setBitSet(new BitSet());
        for (Long offset : ackOffsetList) {
            batchAck.getBitSet().set((int) (offset - startOffset));
        }
        String jsonStr = JSON.toJSONString(batchAck);

        BatchAck bAck = JSON.parseObject(jsonStr, BatchAck.class);
        assertThat(bAck.getConsumerGroup()).isEqualTo(cid);
        assertThat(bAck.getTopic()).isEqualTo(topic);
        assertThat(bAck.getStartOffset()).isEqualTo(startOffset);
        assertThat(bAck.getQueueId()).isEqualTo(qId);
        assertThat(bAck.getReviveQueueId()).isEqualTo(rqId);
        assertThat(bAck.getPopTime()).isEqualTo(popTime);
        assertThat(bAck.getInvisibleTime()).isEqualTo(invisibleTime);
        for (int i = 0; i < bAck.getBitSet().length(); i++) {
            long ackOffset = startOffset + i;
            if (ackOffsetList.contains(ackOffset)) {
                assertThat(bAck.getBitSet().get(i)).isTrue();
            } else {
                assertThat(bAck.getBitSet().get(i)).isFalse();
            }
        }
    }

    @Test
    public void testWithBatchAckMessageRequestBody() {
        List<Long> ackOffsetList = Arrays.asList(startOffset + 1, startOffset + 3, startOffset + 5);
        BatchAck batchAck = new BatchAck();
        batchAck.setConsumerGroup(cid);
        batchAck.setTopic(topic);
        batchAck.setRetry("0");
        batchAck.setStartOffset(startOffset);
        batchAck.setQueueId(qId);
        batchAck.setReviveQueueId(rqId);
        batchAck.setPopTime(popTime);
        batchAck.setInvisibleTime(invisibleTime);
        batchAck.setBitSet(new BitSet());
        for (Long offset : ackOffsetList) {
            batchAck.getBitSet().set((int) (offset - startOffset));
        }

        BatchAckMessageRequestBody batchAckMessageRequestBody = new BatchAckMessageRequestBody();
        batchAckMessageRequestBody.setAcks(Arrays.asList(batchAck));
        byte[] bytes = batchAckMessageRequestBody.encode();
        BatchAckMessageRequestBody reqBody = BatchAckMessageRequestBody.decode(bytes, BatchAckMessageRequestBody.class);
        BatchAck bAck = reqBody.getAcks().get(0);
        assertThat(bAck.getConsumerGroup()).isEqualTo(cid);
        assertThat(bAck.getTopic()).isEqualTo(topic);
        assertThat(bAck.getStartOffset()).isEqualTo(startOffset);
        assertThat(bAck.getQueueId()).isEqualTo(qId);
        assertThat(bAck.getReviveQueueId()).isEqualTo(rqId);
        assertThat(bAck.getPopTime()).isEqualTo(popTime);
        assertThat(bAck.getInvisibleTime()).isEqualTo(invisibleTime);
        for (int i = 0; i < bAck.getBitSet().length(); i++) {
            long ackOffset = startOffset + i;
            if (ackOffsetList.contains(ackOffset)) {
                assertThat(bAck.getBitSet().get(i)).isTrue();
            } else {
                assertThat(bAck.getBitSet().get(i)).isFalse();
            }
        }
    }
}
