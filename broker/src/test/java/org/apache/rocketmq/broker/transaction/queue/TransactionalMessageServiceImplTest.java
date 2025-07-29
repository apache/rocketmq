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
package org.apache.rocketmq.broker.transaction.queue;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.transaction.AbstractTransactionalMessageCheckListener;
import org.apache.rocketmq.broker.transaction.OperationResult;
import org.apache.rocketmq.broker.transaction.TransactionalMessageService;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.client.consumer.PullStatus;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageExtBrokerInner;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.sysflag.MessageSysFlag;
import org.apache.rocketmq.common.topic.TopicValidator;
import org.apache.rocketmq.remoting.netty.NettyClientConfig;
import org.apache.rocketmq.remoting.netty.NettyServerConfig;
import org.apache.rocketmq.remoting.protocol.ResponseCode;
import org.apache.rocketmq.remoting.protocol.header.EndTransactionRequestHeader;
import org.apache.rocketmq.store.AppendMessageResult;
import org.apache.rocketmq.store.AppendMessageStatus;
import org.apache.rocketmq.store.PutMessageResult;
import org.apache.rocketmq.store.PutMessageStatus;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.junit.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.atLeastOnce;

@RunWith(MockitoJUnitRunner.Silent.class)
public class TransactionalMessageServiceImplTest {

    private TransactionalMessageService queueTransactionMsgService;

    @Mock
    private TransactionalMessageBridge bridge;

    @Spy
    private BrokerController brokerController = new BrokerController(new BrokerConfig(), new NettyServerConfig(),
        new NettyClientConfig(), new MessageStoreConfig(), null);

    @Mock
    private AbstractTransactionalMessageCheckListener listener;

    @Before
    public void init() {
        when(bridge.getBrokerController()).thenReturn(brokerController);
        listener.setBrokerController(brokerController);
        queueTransactionMsgService = new TransactionalMessageServiceImpl(bridge);
    }

    @Test
    public void testPrepareMessage() {
        MessageExtBrokerInner inner = createMessageBrokerInner();
        when(bridge.putHalfMessage(any(MessageExtBrokerInner.class)))
                .thenReturn(new PutMessageResult(PutMessageStatus.PUT_OK, new AppendMessageResult(AppendMessageStatus.PUT_OK)));
        PutMessageResult result = queueTransactionMsgService.prepareMessage(inner);
        assert result.isOk();
    }

    @Test
    public void testCommitMessage() {
        when(bridge.lookMessageByOffset(anyLong())).thenReturn(createMessageBrokerInner());
        OperationResult result = queueTransactionMsgService.commitMessage(createEndTransactionRequestHeader(MessageSysFlag.TRANSACTION_COMMIT_TYPE));
        assertThat(result.getResponseCode()).isEqualTo(ResponseCode.SUCCESS);
    }

    @Test
    public void testRollbackMessage() {
        when(bridge.lookMessageByOffset(anyLong())).thenReturn(createMessageBrokerInner());
        OperationResult result = queueTransactionMsgService.commitMessage(createEndTransactionRequestHeader(MessageSysFlag.TRANSACTION_ROLLBACK_TYPE));
        assertThat(result.getResponseCode()).isEqualTo(ResponseCode.SUCCESS);
    }

    @Test
    public void testCheck_withDiscard() {
        when(bridge.fetchMessageQueues(TopicValidator.RMQ_SYS_TRANS_HALF_TOPIC)).thenReturn(createMessageQueueSet(TopicValidator.RMQ_SYS_TRANS_HALF_TOPIC));
        when(bridge.getHalfMessage(0, 0, 1)).thenReturn(createDiscardPullResult(TopicValidator.RMQ_SYS_TRANS_HALF_TOPIC, 5, "hellp", 1));
        when(bridge.getHalfMessage(0, 1, 1)).thenReturn(createPullResult(TopicValidator.RMQ_SYS_TRANS_HALF_TOPIC, 6, "hellp", 0));
        when(bridge.getOpMessage(anyInt(), anyLong(), anyInt())).thenReturn(createOpPulResult(TopicValidator.RMQ_SYS_TRANS_OP_HALF_TOPIC, 1, "10", 1));
        when(bridge.getBrokerController()).thenReturn(this.brokerController);
        long timeOut = this.brokerController.getBrokerConfig().getTransactionTimeOut();
        int checkMax = this.brokerController.getBrokerConfig().getTransactionCheckMax();
        final AtomicInteger checkMessage = new AtomicInteger(0);
        doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocation) {
                checkMessage.addAndGet(1);
                return null;
            }
        }).when(listener).resolveDiscardMsg(any(MessageExt.class));
        queueTransactionMsgService.check(timeOut, checkMax, listener);
        assertThat(checkMessage.get()).isEqualTo(1);
    }

    @Test
    public void testCheck_withCheck() {
        when(bridge.fetchMessageQueues(TopicValidator.RMQ_SYS_TRANS_HALF_TOPIC)).thenReturn(createMessageQueueSet(TopicValidator.RMQ_SYS_TRANS_HALF_TOPIC));
        when(bridge.getHalfMessage(0, 0, 1)).thenReturn(createPullResult(TopicValidator.RMQ_SYS_TRANS_HALF_TOPIC, 5, "hello", 1));
        when(bridge.getHalfMessage(0, 1, 1)).thenReturn(createPullResult(TopicValidator.RMQ_SYS_TRANS_HALF_TOPIC, 6, "hellp", 0));
        when(bridge.getOpMessage(anyInt(), anyLong(), anyInt())).thenReturn(createPullResult(TopicValidator.RMQ_SYS_TRANS_OP_HALF_TOPIC, 1, "5", 0));
        when(bridge.getBrokerController()).thenReturn(this.brokerController);
        when(bridge.renewHalfMessageInner(any(MessageExtBrokerInner.class))).thenReturn(createMessageBrokerInner());
        when(bridge.putMessageReturnResult(any(MessageExtBrokerInner.class)))
                .thenReturn(new PutMessageResult(PutMessageStatus.PUT_OK, new AppendMessageResult(AppendMessageStatus.PUT_OK)));
        long timeOut = this.brokerController.getBrokerConfig().getTransactionTimeOut();
        final int checkMax = this.brokerController.getBrokerConfig().getTransactionCheckMax();
        final AtomicInteger checkMessage = new AtomicInteger(0);
        doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocation) {
                checkMessage.addAndGet(1);
                return checkMessage;
            }
        }).when(listener).resolveHalfMsg(any(MessageExt.class));
        queueTransactionMsgService.check(timeOut, checkMax, listener);
        assertThat(checkMessage.get()).isEqualTo(1);
    }

    @Test
    public void testDeletePrepareMessage_queueFull() throws InterruptedException {
        ((TransactionalMessageServiceImpl)queueTransactionMsgService).getDeleteContext().put(0, new MessageQueueOpContext(0, 1));
        boolean res = queueTransactionMsgService.deletePrepareMessage(createMessageBrokerInner());
        assertThat(res).isTrue();
        when(bridge.writeOp(any(Integer.class), any(Message.class))).thenReturn(false);
        res = queueTransactionMsgService.deletePrepareMessage(createMessageBrokerInner());
        assertThat(res).isFalse();
    }

    @Test
    public void testDeletePrepareMessage_maxSize() throws InterruptedException {
        brokerController.getBrokerConfig().setTransactionOpMsgMaxSize(1);
        brokerController.getBrokerConfig().setTransactionOpBatchInterval(3000);
        queueTransactionMsgService.open();
        boolean res = queueTransactionMsgService.deletePrepareMessage(createMessageBrokerInner(1000, "test", "testHello"));
        assertThat(res).isTrue();
        verify(bridge, timeout(50)).writeOp(any(Integer.class), any(Message.class));
        queueTransactionMsgService.close();
    }

    @Test
    public void testOpen() {
        boolean isOpen = queueTransactionMsgService.open();
        assertThat(isOpen).isTrue();
    }

    /**
     * Test for Issue #9500: Verify that offsets are advanced even when exceptions occur during transaction check
     * This test ensures that the infinite loop problem is fixed by advancing offsets in finally block
     */
    @Test
    public void testCheck_OffsetAdvancementOnException() {
        // Setup message queues
        when(bridge.fetchMessageQueues(TopicValidator.RMQ_SYS_TRANS_HALF_TOPIC))
                .thenReturn(createMessageQueueSet(TopicValidator.RMQ_SYS_TRANS_HALF_TOPIC));
        
        // Setup initial offsets
        when(bridge.fetchConsumeOffset(any(MessageQueue.class))).thenReturn(0L);
        
        // Setup half message with proper properties
        MessageExtBrokerInner halfMessage = createMessageBrokerInner(0, TopicValidator.RMQ_SYS_TRANS_HALF_TOPIC, "test");
        halfMessage.setTopic("testTopic");
        halfMessage.setMsgId("testMsgId");
        halfMessage.setBornTimestamp(System.currentTimeMillis() - 120000); // Old enough to be checked
        
        when(bridge.getHalfMessage(anyInt(), anyLong(), anyInt()))
                .thenReturn(createPullResultWithMessage(TopicValidator.RMQ_SYS_TRANS_HALF_TOPIC, 0, halfMessage));
        
        // Setup op message with proper format
        MessageExtBrokerInner opMessage = createMessageBrokerInner(0, TopicValidator.RMQ_SYS_TRANS_OP_HALF_TOPIC, "0");
        opMessage.setTags(TransactionalMessageUtil.REMOVE_TAG);
        
        when(bridge.getOpMessage(anyInt(), anyLong(), anyInt()))
                .thenReturn(createPullResultWithMessage(TopicValidator.RMQ_SYS_TRANS_OP_HALF_TOPIC, 0, opMessage));
        
        // Mock the bridge to throw exception during transaction check
        when(bridge.getBrokerController()).thenReturn(this.brokerController);
        when(bridge.renewHalfMessageInner(any(MessageExtBrokerInner.class)))
                .thenThrow(new RuntimeException("Simulated exception during transaction check"));
        
        // Mock other necessary methods
        when(bridge.putMessageReturnResult(any(MessageExtBrokerInner.class)))
                .thenReturn(new PutMessageResult(PutMessageStatus.PUT_OK, new AppendMessageResult(AppendMessageStatus.PUT_OK)));
        
        long timeOut = this.brokerController.getBrokerConfig().getTransactionTimeOut();
        int checkMax = this.brokerController.getBrokerConfig().getTransactionCheckMax();
        
        // Execute the check method - this should not throw exception due to try-catch
        queueTransactionMsgService.check(timeOut, checkMax, listener);
        
        // Key assertion: updateConsumeOffset must be called even if exception occurs
        verify(bridge, atLeastOnce()).updateConsumeOffset(any(MessageQueue.class), anyLong());
    }

    /**
     * Test normal case: offset should be advanced when no exception occurs
     */
    @Test
    public void testCheck_OffsetAdvancementOnNormal() {
        // Setup message queues
        when(bridge.fetchMessageQueues(TopicValidator.RMQ_SYS_TRANS_HALF_TOPIC))
                .thenReturn(createMessageQueueSet(TopicValidator.RMQ_SYS_TRANS_HALF_TOPIC));
        
        // Setup initial offsets
        when(bridge.fetchConsumeOffset(any(MessageQueue.class))).thenReturn(0L);
        
        // Setup half message with proper properties
        MessageExtBrokerInner halfMessage = createMessageBrokerInner(0, TopicValidator.RMQ_SYS_TRANS_HALF_TOPIC, "test");
        halfMessage.setTopic("testTopic");
        halfMessage.setMsgId("testMsgId");
        halfMessage.setBornTimestamp(System.currentTimeMillis() - 120000); // Old enough to be checked
        
        when(bridge.getHalfMessage(anyInt(), anyLong(), anyInt()))
                .thenReturn(createPullResultWithMessage(TopicValidator.RMQ_SYS_TRANS_HALF_TOPIC, 0, halfMessage));
        
        // Setup op message with proper format
        MessageExtBrokerInner opMessage = createMessageBrokerInner(0, TopicValidator.RMQ_SYS_TRANS_OP_HALF_TOPIC, "0");
        opMessage.setTags(TransactionalMessageUtil.REMOVE_TAG);
        
        when(bridge.getOpMessage(anyInt(), anyLong(), anyInt()))
                .thenReturn(createPullResultWithMessage(TopicValidator.RMQ_SYS_TRANS_OP_HALF_TOPIC, 0, opMessage));
        
        when(bridge.getBrokerController()).thenReturn(this.brokerController);
        
        // No exception thrown, return a mock MessageExtBrokerInner
        when(bridge.renewHalfMessageInner(any(MessageExtBrokerInner.class)))
                .thenReturn(org.mockito.Mockito.mock(MessageExtBrokerInner.class));
        
        // Mock other necessary methods
        when(bridge.putMessageReturnResult(any(MessageExtBrokerInner.class)))
                .thenReturn(new PutMessageResult(PutMessageStatus.PUT_OK, new AppendMessageResult(AppendMessageStatus.PUT_OK)));
        
        long timeOut = this.brokerController.getBrokerConfig().getTransactionTimeOut();
        int checkMax = this.brokerController.getBrokerConfig().getTransactionCheckMax();
        
        queueTransactionMsgService.check(timeOut, checkMax, listener);
        
        // Key assertion: updateConsumeOffset must be called
        verify(bridge, atLeastOnce()).updateConsumeOffset(any(MessageQueue.class), anyLong());
    }

    /**
     * Test empty queue: offset should not be advanced if no queue exists
     */
    @Test
    public void testCheck_EmptyQueueNoOffsetUpdate() {
        // Setup empty message queue set
        when(bridge.fetchMessageQueues(TopicValidator.RMQ_SYS_TRANS_HALF_TOPIC))
                .thenReturn(new java.util.HashSet<>());
        
        long timeOut = this.brokerController.getBrokerConfig().getTransactionTimeOut();
        int checkMax = this.brokerController.getBrokerConfig().getTransactionCheckMax();
        
        queueTransactionMsgService.check(timeOut, checkMax, listener);
        
        // Key assertion: updateConsumeOffset should never be called
        verify(bridge, org.mockito.Mockito.never()).updateConsumeOffset(any(MessageQueue.class), anyLong());
    }

    /**
     * Test transaction check with proper message flow
     */
    @Test
    public void testCheck_WithProperMessageFlow() {
        // Setup message queues
        when(bridge.fetchMessageQueues(TopicValidator.RMQ_SYS_TRANS_HALF_TOPIC))
                .thenReturn(createMessageQueueSet(TopicValidator.RMQ_SYS_TRANS_HALF_TOPIC));
        
        // Setup initial offsets
        when(bridge.fetchConsumeOffset(any(MessageQueue.class))).thenReturn(0L);
        
        // Setup half message that needs to be checked
        MessageExtBrokerInner halfMessage = createMessageBrokerInner(0, TopicValidator.RMQ_SYS_TRANS_HALF_TOPIC, "test");
        halfMessage.setTopic("testTopic");
        halfMessage.setMsgId("testMsgId");
        halfMessage.setBornTimestamp(System.currentTimeMillis() - 120000); // Old enough to be checked
        
        // Mock getHalfMessage to return message first time, then empty
        when(bridge.getHalfMessage(anyInt(), eq(0L), anyInt()))
                .thenReturn(createPullResultWithMessage(TopicValidator.RMQ_SYS_TRANS_HALF_TOPIC, 0, halfMessage));
        when(bridge.getHalfMessage(anyInt(), eq(1L), anyInt()))
                .thenReturn(createPullResult(TopicValidator.RMQ_SYS_TRANS_HALF_TOPIC, 1, "", 0));
        
        // Setup empty op message (no op message found)
        when(bridge.getOpMessage(anyInt(), anyLong(), anyInt()))
                .thenReturn(createPullResult(TopicValidator.RMQ_SYS_TRANS_OP_HALF_TOPIC, 0, "", 0));
        
        when(bridge.getBrokerController()).thenReturn(this.brokerController);
        
        // Mock renewHalfMessageInner to return a proper message
        MessageExtBrokerInner renewedMessage = createMessageBrokerInner(0, TopicValidator.RMQ_SYS_TRANS_HALF_TOPIC, "test");
        when(bridge.renewHalfMessageInner(any(MessageExtBrokerInner.class)))
                .thenReturn(renewedMessage);
        
        // Mock putMessageReturnResult to return success
        when(bridge.putMessageReturnResult(any(MessageExtBrokerInner.class)))
                .thenReturn(new PutMessageResult(PutMessageStatus.PUT_OK, new AppendMessageResult(AppendMessageStatus.PUT_OK)));
        
        // Mock listener to track calls
        final AtomicInteger resolveHalfMsgCount = new AtomicInteger(0);
        doAnswer(new Answer<Object>() {
            @Override
            public Object answer(InvocationOnMock invocation) {
                resolveHalfMsgCount.addAndGet(1);
                return null;
            }
        }).when(listener).resolveHalfMsg(any(MessageExt.class));
        
        long timeOut = this.brokerController.getBrokerConfig().getTransactionTimeOut();
        int checkMax = this.brokerController.getBrokerConfig().getTransactionCheckMax();
        
        queueTransactionMsgService.check(timeOut, checkMax, listener);
        
        // Verify that resolveHalfMsg was called (message was checked)
        assertThat(resolveHalfMsgCount.get()).isEqualTo(1);
        
        // Verify that offsets were updated
        verify(bridge, atLeastOnce()).updateConsumeOffset(any(MessageQueue.class), anyLong());
    }

    private PullResult createDiscardPullResult(String topic, long queueOffset, String body, int size) {
        PullResult result = createPullResult(topic, queueOffset, body, size);
        List<MessageExt> msgs = result.getMsgFoundList();
        for (MessageExt msg : msgs) {
            msg.putUserProperty(MessageConst.PROPERTY_TRANSACTION_CHECK_TIMES, "100000");
        }
        return result;
    }

    private PullResult createPullResult(String topic, long queueOffset, String body, int size) {
        PullResult result = null;
        if (0 == size) {
            result = new PullResult(PullStatus.NO_NEW_MSG, 1, 0, 1,
                null);
        } else {
            result = new PullResult(PullStatus.FOUND, 1, 0, 1,
                getMessageList(queueOffset, topic, body, 1));
            return result;
        }
        return result;
    }

    private PullResult createOpPulResult(String topic, long queueOffset, String body, int size) {
        PullResult result = createPullResult(topic, queueOffset, body, size);
        List<MessageExt> msgs = result.getMsgFoundList();
        for (MessageExt msg : msgs) {
            msg.setTags(TransactionalMessageUtil.REMOVE_TAG);
        }
        return result;
    }

    private PullResult createImmunityPulResult(String topic, long queueOffset, String body, int size) {
        PullResult result = createPullResult(topic, queueOffset, body, size);
        List<MessageExt> msgs = result.getMsgFoundList();
        for (MessageExt msg : msgs) {
            msg.putUserProperty(MessageConst.PROPERTY_CHECK_IMMUNITY_TIME_IN_SECONDS, "0");
        }
        return result;
    }

    private List<MessageExt> getMessageList(long queueOffset, String topic, String body, int size) {
        List<MessageExt> msgs = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            MessageExt messageExt = createMessageBrokerInner(queueOffset, topic, body);
            msgs.add(messageExt);
        }
        return msgs;
    }

    private Set<MessageQueue> createMessageQueueSet(String topic) {
        Set<MessageQueue> messageQueues = new HashSet<>();
        MessageQueue messageQueue = new MessageQueue(topic, "DefaultCluster", 0);
        messageQueues.add(messageQueue);
        return messageQueues;
    }

    private EndTransactionRequestHeader createEndTransactionRequestHeader(int status) {
        EndTransactionRequestHeader header = new EndTransactionRequestHeader();
        header.setTopic("topic");
        header.setCommitLogOffset(123456789L);
        header.setCommitOrRollback(status);
        header.setMsgId("12345678");
        header.setTransactionId("123");
        header.setProducerGroup("testTransactionGroup");
        header.setTranStateTableOffset(1234L);
        return header;
    }

    private MessageExtBrokerInner createMessageBrokerInner(long queueOffset, String topic, String body) {
        MessageExtBrokerInner inner = new MessageExtBrokerInner();
        inner.setBornTimestamp(System.currentTimeMillis() - 80000);
        inner.setTransactionId("123456123");
        inner.setTopic(topic);
        inner.setQueueOffset(queueOffset);
        inner.setBody(body.getBytes());
        inner.setMsgId("123456123");
        inner.setQueueId(0);
        inner.setTopic("hello");
        return inner;
    }

    private MessageExtBrokerInner createMessageBrokerInner() {
        return createMessageBrokerInner(1, "testTopic", "hello world");
    }

    private PullResult createPullResultWithMessage(String topic, long queueOffset, MessageExtBrokerInner message) {
        List<MessageExt> messages = new ArrayList<>();
        messages.add(message);
        return new PullResult(PullStatus.FOUND, 1, 0, 1, messages);
    }
}
