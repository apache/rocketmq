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

package org.apache.rocketmq.client.trace.hook;

import org.apache.rocketmq.client.hook.SendMessageContext;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.client.trace.AsyncTraceDispatcher;
import org.apache.rocketmq.client.trace.TraceBean;
import org.apache.rocketmq.client.trace.TraceContext;
import org.apache.rocketmq.client.trace.TraceDispatcher;
import org.apache.rocketmq.client.trace.TraceType;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageBatch;
import org.apache.rocketmq.common.message.MessageType;
import org.apache.rocketmq.common.topic.TopicValidator;
import org.apache.rocketmq.remoting.RPCHook;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;

public class SendMessageTraceHookImplTest {

    private static final String TOPIC = "TopicTest";
    private static final String TAGS = "tags";
    private static final String KEYS = "keys";
    private static final String BODY = "bodyLength";
    private static final String BROKER = "127.0.0.1:10911";
    private static final String GROUP = "producer";

    private static final String MSG_ID = "msgId";
    private static final String OFFSET_MSG_ID = "offsetMsgId";
    private static final String REGION_ID = "testRegion";

    private SendMessageTraceHookImpl sendMessageTraceHook;
    private TestAsyncTraceDispatcher traceDispatcher;

    private static class TestAsyncTraceDispatcher extends AsyncTraceDispatcher {

        private BlockingDeque<TraceContext> stashQueue = new LinkedBlockingDeque<>(1024);

        public TestAsyncTraceDispatcher(String group, Type type, String traceTopicName, RPCHook rpcHook) {
            super(group, type, traceTopicName, rpcHook);
        }

        @Override
        public boolean append(Object ctx) {
            return stashQueue.offer((TraceContext) ctx);
        }

        public BlockingDeque<TraceContext> getStashQueue() {
            return stashQueue;
        }
    }

    @Before
    public void init() {
        traceDispatcher = new TestAsyncTraceDispatcher(null, TraceDispatcher.Type.PRODUCE, TopicValidator.RMQ_SYS_TRACE_TOPIC, null);
        sendMessageTraceHook = new SendMessageTraceHookImpl(traceDispatcher);
    }

    @Test
    public void testBuildBeforeTraceBean() {
        TraceBean traceBean = sendMessageTraceHook.buildBeforeTraceBean(buildTestMessage(), MessageType.Normal_Msg, BROKER);
        Assert.assertEquals(traceBean.getTopic(), TOPIC);
        Assert.assertEquals(traceBean.getTags(), TAGS);
        Assert.assertEquals(traceBean.getKeys(), KEYS);
        Assert.assertEquals(traceBean.getBodyLength(), BODY.length());
        Assert.assertEquals(traceBean.getMsgType(), MessageType.Normal_Msg);
        Assert.assertEquals(traceBean.getStoreHost(), BROKER);
    }

    @Test
    public void testSendSingleMessageTrace() throws InterruptedException {
        SendMessageContext context = new SendMessageContext();
        context.setMessage(buildTestMessage());
        context.setMsgType(MessageType.Normal_Msg);
        context.setBrokerAddr(BROKER);
        context.setProducerGroup(GROUP);

        sendMessageTraceHook.sendMessageBefore(context);
        Thread.sleep(100);
        context.setSendResult(buildTestResult());
        sendMessageTraceHook.sendMessageAfter(context);

        Assert.assertEquals(traceDispatcher.getStashQueue().size(), 1);
        TraceContext traceContext = traceDispatcher.getStashQueue().poll();
        Assert.assertNotNull(traceContext);
        Assert.assertNotNull(traceContext.getTraceBeans());
        Assert.assertEquals(traceContext.getTraceBeans().size(), 1);
        Assert.assertEquals(traceContext.getTraceType(), TraceType.Pub);
        Assert.assertEquals(traceContext.getRegionId(), REGION_ID);
        Assert.assertEquals(traceContext.getGroupName(), GROUP);
        Assert.assertTrue(traceContext.getCostTime() >= 100 && traceContext.getCostTime() < 200);
        Assert.assertTrue(traceContext.isSuccess());
        Assert.assertEquals(traceContext.getTraceBeans().size(), 1);

        TraceBean traceBean = traceContext.getTraceBeans().get(0);
        Assert.assertEquals(traceBean.getTopic(), TOPIC);
        Assert.assertEquals(traceBean.getTags(), TAGS);
        Assert.assertEquals(traceBean.getKeys(), KEYS);
        Assert.assertEquals(traceBean.getBodyLength(), BODY.length());
        Assert.assertEquals(traceBean.getMsgType(), MessageType.Normal_Msg);
        Assert.assertEquals(traceBean.getStoreHost(), BROKER);
    }

    @Test
    public void testSendBatchMessageTrace() throws InterruptedException {
        int batchSize = 10;
        SendMessageContext context = new SendMessageContext();
        context.setMessage(buildTestBatchMessage(batchSize));
        context.setMsgType(MessageType.Normal_Msg);
        context.setBrokerAddr(BROKER);
        context.setProducerGroup(GROUP);

        sendMessageTraceHook.sendMessageBefore(context);
        Thread.sleep(100);
        context.setSendResult(buildTestBatchResult(batchSize));
        sendMessageTraceHook.sendMessageAfter(context);

        Assert.assertEquals(traceDispatcher.getStashQueue().size(), batchSize);
        for (int i = 0; i < batchSize; i++) {
            TraceContext traceContext = traceDispatcher.getStashQueue().poll();
            Assert.assertNotNull(traceContext);
            Assert.assertNotNull(traceContext.getTraceBeans());
            Assert.assertEquals(traceContext.getTraceBeans().size(), 1);
            Assert.assertEquals(traceContext.getTraceType(), TraceType.Pub);
            Assert.assertEquals(traceContext.getRegionId(), REGION_ID);
            Assert.assertEquals(traceContext.getGroupName(), GROUP);
            Assert.assertTrue(traceContext.getCostTime() >= 100 && traceContext.getCostTime() < 200);
            Assert.assertTrue(traceContext.isSuccess());
            Assert.assertEquals(traceContext.getTraceBeans().size(), 1);

            TraceBean traceBean = traceContext.getTraceBeans().get(0);
            Assert.assertEquals(traceBean.getTopic(), TOPIC);
            Assert.assertEquals(traceBean.getTags(), TAGS + i);
            Assert.assertEquals(traceBean.getKeys(), KEYS + i);
            Assert.assertEquals(traceBean.getBodyLength(), (BODY + i).length());
            Assert.assertEquals(traceBean.getMsgType(), MessageType.Normal_Msg);
            Assert.assertEquals(traceBean.getStoreHost(), BROKER);
        }
    }

    private Message buildTestMessage() {
        Message message = new Message();
        message.setTopic(TOPIC);
        message.setTags(TAGS);
        message.setKeys(KEYS);
        message.setBody(BODY.getBytes(StandardCharsets.UTF_8));
        return message;
    }

    private SendResult buildTestResult() {
        SendResult sendResult = new SendResult();
        sendResult.setMsgId(MSG_ID);
        sendResult.setOffsetMsgId(OFFSET_MSG_ID);
        sendResult.setSendStatus(SendStatus.SEND_OK);
        sendResult.setRegionId(REGION_ID);
        return sendResult;
    }

    private Message buildTestBatchMessage(int batchSize) {
        List<Message> messages = new ArrayList<>(batchSize);
        for (int i = 0; i < batchSize; i++) {
            Message message = new Message();
            message.setTopic(TOPIC);
            message.setTags(TAGS + i);
            message.setKeys(KEYS + i);
            message.setBody((BODY + i).getBytes(StandardCharsets.UTF_8));
            messages.add(message);
        }
        return MessageBatch.generateFromList(messages);
    }

    private SendResult buildTestBatchResult(int batchSize) {
        StringBuilder batchMsgId = new StringBuilder();
        StringBuilder batchOffsetMsgId = new StringBuilder();
        for (int i = 0; i < batchSize; i++) {
            batchMsgId.append(MSG_ID);
            batchOffsetMsgId.append(OFFSET_MSG_ID);
            if (i < batchSize - 1) {
                batchMsgId.append(",");
                batchOffsetMsgId.append(",");
            }
        }

        SendResult sendResult = new SendResult();
        sendResult.setMsgId(batchMsgId.toString());
        sendResult.setOffsetMsgId(batchOffsetMsgId.toString());
        sendResult.setSendStatus(SendStatus.SEND_OK);
        sendResult.setRegionId(REGION_ID);
        return sendResult;
    }

}
