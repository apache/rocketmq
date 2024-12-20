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

package org.apache.rocketmq.common.producer;

import org.apache.commons.codec.DecoderException;
import org.apache.rocketmq.common.message.MessageClientIDSetter;
import org.junit.Assert;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

public class RecallMessageHandleTest {
    @Test
    public void testHandleInvalid() {
        Assert.assertThrows(DecoderException.class, () -> {
            RecallMessageHandle.decodeHandle("");
        });
        Assert.assertThrows(DecoderException.class, () -> {
            RecallMessageHandle.decodeHandle(null);
        });

        Assert.assertThrows(DecoderException.class, () -> {
            String invalidHandle = Base64.getUrlEncoder().encodeToString("v1 a b c".getBytes(StandardCharsets.UTF_8));
            RecallMessageHandle.decodeHandle(invalidHandle);
        });
        Assert.assertThrows(DecoderException.class, () -> {
            String invalidHandle = Base64.getUrlEncoder().encodeToString("v2 a b c d".getBytes(StandardCharsets.UTF_8));
            RecallMessageHandle.decodeHandle(invalidHandle);
        });
        Assert.assertThrows(DecoderException.class, () -> {
            String invalidHandle = "v1 a b c d";
            RecallMessageHandle.decodeHandle(invalidHandle);
        });
    }

    @Test
    public void testEncodeAndDecodeV1() throws DecoderException {
        String topic = "topic";
        String brokerName = "broker-0";
        String timestampStr = String.valueOf(System.currentTimeMillis());
        String messageId = MessageClientIDSetter.createUniqID();
        String handle = RecallMessageHandle.HandleV1.buildHandle(topic, brokerName, timestampStr, messageId);
        RecallMessageHandle handleEntity = RecallMessageHandle.decodeHandle(handle);
        Assert.assertTrue(handleEntity instanceof RecallMessageHandle.HandleV1);
        RecallMessageHandle.HandleV1 handleV1 = (RecallMessageHandle.HandleV1) handleEntity;
        Assert.assertEquals(handleV1.getVersion(), "v1");
        Assert.assertEquals(handleV1.getTopic(), topic);
        Assert.assertEquals(handleV1.getBrokerName(), brokerName);
        Assert.assertEquals(handleV1.getTimestampStr(), timestampStr);
        Assert.assertEquals(handleV1.getMessageId(), messageId);
    }
}
