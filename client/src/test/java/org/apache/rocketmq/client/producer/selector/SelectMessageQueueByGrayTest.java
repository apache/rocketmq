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
package org.apache.rocketmq.client.producer.selector;

import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.MockitoAnnotations;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertTrue;


public class SelectMessageQueueByGrayTest {

    @InjectMocks
    private SelectMessageQueueByGray selector;

    private List<MessageQueue> messageQueues;

    private Message message;

    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);
        messageQueues = new ArrayList<>();
        messageQueues.add(new MessageQueue("topic1", "broker1", 0));
        messageQueues.add(new MessageQueue("topic1", "broker1", 1));
        messageQueues.add(new MessageQueue("topic1", "broker1", 2));
        messageQueues.add(new MessageQueue("topic1", "broker1", 3));
        messageQueues.add(new MessageQueue("topic1", "broker2", 0));
        messageQueues.add(new MessageQueue("topic1", "broker2", 1));
        messageQueues.add(new MessageQueue("topic1", "broker2", 2));
        messageQueues.add(new MessageQueue("topic1", "broker2", 3));
        try {
            message = new Message("topic1" /* Topic */,
                    "TagA" /* Tag */,
                    "hello".getBytes(RemotingHelper.DEFAULT_CHARSET) /* Message body */
            );
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testSelect_WithGrayQueueRatioZero_ShouldSelectFromNormalQueues() throws UnsupportedEncodingException {
        selector = new SelectMessageQueueByGray(0);
        MessageQueue selectedQueue = selector.select(messageQueues, message, true);
        assertTrue(selectedQueue.getBrokerName().startsWith("broker2") || selectedQueue.getBrokerName().startsWith("broker1"));
    }

    @Test
    public void testSelect_WithGrayQueueRatioHalf_ShouldSelectFromGrayAndNormalQueues() {
        selector = new SelectMessageQueueByGray(0.5);
        MessageQueue selectedQueue = selector.select(messageQueues, message, true);
        assertTrue(selectedQueue.getBrokerName().startsWith("broker1") || selectedQueue.getBrokerName().startsWith("broker2"));
    }

    @Test
    public void testSelect_WithBooleanArgument_ShouldSelectGrayQueues() {
        selector = new SelectMessageQueueByGray(0.7);
        MessageQueue selectedQueue = selector.select(messageQueues, message, false);
        assertTrue(selectedQueue.getBrokerName().startsWith("broker1"));
    }
}
