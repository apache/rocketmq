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
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.rocketmq.common;

import java.util.ArrayList;
import java.util.List;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageBatch;
import org.junit.Test;

public class MessageBatchTest {

    public List<Message> generateMessages() {
        List<Message> messages = new ArrayList<>();
        Message message1 = new Message("topic1", "body".getBytes());
        Message message2 = new Message("topic1", "body".getBytes());

        messages.add(message1);
        messages.add(message2);
        return messages;
    }

    @Test
    public void testGenerate_OK() throws Exception {
        List<Message> messages = generateMessages();
        MessageBatch.generateFromList(messages);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGenerate_DiffTopic() throws Exception {
        List<Message> messages = generateMessages();
        messages.get(1).setTopic("topic2");
        MessageBatch.generateFromList(messages);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGenerate_DiffWaitOK() throws Exception {
        List<Message> messages = generateMessages();
        messages.get(1).setWaitStoreMsgOK(false);
        MessageBatch.generateFromList(messages);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGenerate_Delay() throws Exception {
        List<Message> messages = generateMessages();
        messages.get(1).setDelayTimeLevel(1);
        MessageBatch.generateFromList(messages);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGenerate_Retry() throws Exception {
        List<Message> messages = generateMessages();
        messages.get(1).setTopic(MixAll.RETRY_GROUP_TOPIC_PREFIX + "topic");
        MessageBatch.generateFromList(messages);
    }
}
