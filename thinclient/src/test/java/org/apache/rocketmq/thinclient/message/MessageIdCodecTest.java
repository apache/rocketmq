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

package org.apache.rocketmq.thinclient.message;

import org.apache.rocketmq.apis.message.MessageId;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;

public class MessageIdCodecTest {
    private final MessageIdCodec codec = MessageIdCodec.getInstance();

    @Test
    public void testNextMessageId() {
        final MessageId messageId = codec.nextMessageId();
        Assert.assertEquals(MessageIdCodec.MESSAGE_ID_LENGTH_FOR_V1_OR_LATER, messageId.toString().length());
    }

    @Test
    public void testNextMessageIdWithNoRepetition() {
        Set<MessageId> messageIds = new HashSet<>();
        int messageIdCount = 64;
        for (int i = 0; i < messageIdCount; i++) {
            messageIds.add(codec.nextMessageId());
        }
        Assert.assertEquals(messageIdCount, messageIds.size());
    }

    @Test
    public void testDecode() {
        String messageIdString = "0156F7E71C361B21BC024CCDBE00000000";
        final MessageId messageId = codec.decode(messageIdString);
        Assert.assertEquals(MessageIdCodec.MESSAGE_ID_VERSION_V1, messageId.getVersion());
        Assert.assertEquals(messageIdString, messageId.toString());
    }
}