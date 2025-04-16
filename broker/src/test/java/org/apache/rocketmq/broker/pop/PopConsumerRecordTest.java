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

import java.util.UUID;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.junit.Assert;
import org.junit.Test;

public class PopConsumerRecordTest {

    private static final Logger log = LoggerFactory.getLogger(LoggerName.ROCKETMQ_POP_LOGGER_NAME);

    @Test
    public void retryCodeTest() {
        Assert.assertEquals("NORMAL_TOPIC code should be 0",
            0, PopConsumerRecord.RetryType.NORMAL_TOPIC.getCode());
        Assert.assertEquals("RETRY_TOPIC code should be 1",
            1, PopConsumerRecord.RetryType.RETRY_TOPIC_V1.getCode());
        Assert.assertEquals("RETRY_TOPIC_V2 code should be 2",
            2, PopConsumerRecord.RetryType.RETRY_TOPIC_V2.getCode());
    }

    @Test
    public void deliveryRecordSerializeTest() {
        PopConsumerRecord consumerRecord = new PopConsumerRecord();
        consumerRecord.setPopTime(System.currentTimeMillis());
        consumerRecord.setGroupId("GroupId");
        consumerRecord.setTopicId("TopicId");
        consumerRecord.setQueueId(3);
        consumerRecord.setRetryFlag(PopConsumerRecord.RetryType.RETRY_TOPIC_V1.getCode());
        consumerRecord.setInvisibleTime(20);
        consumerRecord.setOffset(100);
        consumerRecord.setAttemptTimes(2);
        consumerRecord.setAttemptId(UUID.randomUUID().toString().toUpperCase());

        Assert.assertTrue(consumerRecord.isRetry());
        Assert.assertEquals(consumerRecord.getPopTime() + consumerRecord.getInvisibleTime(),
            consumerRecord.getVisibilityTimeout());
        Assert.assertEquals(8 + "GroupId".length() + 1 + "TopicId".length() + 1 + 4 + 1 + 8,
            consumerRecord.getKeyBytes().length);
        log.info("ConsumerRecord={}", consumerRecord.toString());

        PopConsumerRecord decodeRecord = PopConsumerRecord.decode(consumerRecord.getValueBytes());
        PopConsumerRecord consumerRecord2 = new PopConsumerRecord(consumerRecord.getPopTime(),
            consumerRecord.getGroupId(), consumerRecord.getTopicId(), consumerRecord.getQueueId(),
            consumerRecord.getRetryFlag(), consumerRecord.getInvisibleTime(),
            consumerRecord.getOffset(), consumerRecord.getAttemptId());
        Assert.assertEquals(decodeRecord.getPopTime(), consumerRecord2.getPopTime());
        Assert.assertEquals(decodeRecord.getGroupId(), consumerRecord2.getGroupId());
        Assert.assertEquals(decodeRecord.getTopicId(), consumerRecord2.getTopicId());
        Assert.assertEquals(decodeRecord.getQueueId(), consumerRecord2.getQueueId());
        Assert.assertEquals(decodeRecord.getRetryFlag(), consumerRecord2.getRetryFlag());
        Assert.assertEquals(decodeRecord.getInvisibleTime(), consumerRecord2.getInvisibleTime());
        Assert.assertEquals(decodeRecord.getOffset(), consumerRecord2.getOffset());
        Assert.assertEquals(0, consumerRecord2.getAttemptTimes());
        Assert.assertEquals(decodeRecord.getAttemptId(), consumerRecord2.getAttemptId());
    }
}