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

package org.apache.rocketmq.client.producer;

import com.alibaba.fastjson2.JSON;
import org.apache.rocketmq.common.message.MessageQueue;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class SendResultTest {

    @Test
    public void testEncoderSendResultToJson() {
        SendResult sendResult = new SendResult();
        sendResult.setSendStatus(SendStatus.SEND_OK);
        sendResult.setMsgId("12345");
        sendResult.setQueueOffset(100L);
        MessageQueue messageQueue = new MessageQueue("TestTopic", "BrokerA", 1);
        sendResult.setMessageQueue(messageQueue);

        String json = SendResult.encoderSendResultToJson(sendResult);

        SendResult decodedResult = JSON.parseObject(json, SendResult.class);
        assertEquals(sendResult.getSendStatus(), decodedResult.getSendStatus());
        assertEquals(sendResult.getMsgId(), decodedResult.getMsgId());
        assertEquals(sendResult.getQueueOffset(), decodedResult.getQueueOffset());
        assertEquals(sendResult.getMessageQueue(), decodedResult.getMessageQueue());
    }

    @Test
    public void testDecoderSendResultFromJson() {
        String json = "{\"sendStatus\":\"SEND_OK\",\"msgId\":\"12345\",\"queueOffset\":100,\"messageQueue\":{\"topic\":\"TestTopic\",\"brokerName\":\"BrokerA\",\"queueId\":1}}";

        SendResult sendResult = SendResult.decoderSendResultFromJson(json);

        assertEquals(SendStatus.SEND_OK, sendResult.getSendStatus());
        assertEquals("12345", sendResult.getMsgId());
        assertEquals(100L, sendResult.getQueueOffset());
        assertEquals("TestTopic", sendResult.getMessageQueue().getTopic());
        assertEquals("BrokerA", sendResult.getMessageQueue().getBrokerName());
        assertEquals(1, sendResult.getMessageQueue().getQueueId());
    }
}
