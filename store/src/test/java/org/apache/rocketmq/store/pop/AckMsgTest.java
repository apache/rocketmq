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

import com.alibaba.fastjson.JSON;
import org.junit.Assert;
import org.junit.Test;

public class AckMsgTest {

    @Test
    public void testSerializeAndDeSerialize() {
        String longString = "{\"ackOffset\":100,\"brokerName\":\"brokerName\",\"consumerGroup\":\"group\"," +
            "\"popTime\":1670212915531,\"queueId\":3,\"startOffset\":200,\"topic\":\"topic\"}";

        AckMsg ackMsg = new AckMsg();
        ackMsg.setBrokerName("brokerName");
        ackMsg.setTopic("topic");
        ackMsg.setConsumerGroup("group");
        ackMsg.setQueueId(3);
        ackMsg.setStartOffset(200L);
        ackMsg.setAckOffset(100L);
        ackMsg.setPopTime(1670212915531L);
        String jsonString = JSON.toJSONString(ackMsg);
        AckMsg ackMsg1 = JSON.parseObject(jsonString, AckMsg.class);
        AckMsg ackMsg2 = JSON.parseObject(longString, AckMsg.class);

        Assert.assertEquals(ackMsg1.getBrokerName(), ackMsg2.getBrokerName());
        Assert.assertEquals(ackMsg1.getTopic(), ackMsg2.getTopic());
        Assert.assertEquals(ackMsg1.getConsumerGroup(), ackMsg2.getConsumerGroup());
        Assert.assertEquals(ackMsg1.getQueueId(), ackMsg2.getQueueId());
        Assert.assertEquals(ackMsg1.getStartOffset(), ackMsg2.getStartOffset());
        Assert.assertEquals(ackMsg1.getAckOffset(), ackMsg2.getAckOffset());
        Assert.assertEquals(ackMsg1.getPopTime(), ackMsg2.getPopTime());
    }
}