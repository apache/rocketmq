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
package org.apache.rocketmq.broker.processor;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.client.ClientChannelInfo;
import org.apache.rocketmq.broker.pagecache.BatchManyMessageTransfer;
import org.apache.rocketmq.broker.pagecache.ManyMessageTransfer;
import org.apache.rocketmq.broker.subscription.SubscriptionGroupManager;
import org.apache.rocketmq.broker.topic.TopicConfigManager;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.protocol.RequestCode;
import org.apache.rocketmq.common.protocol.ResponseCode;
import org.apache.rocketmq.common.protocol.header.PullMessageResponseHeader;
import org.apache.rocketmq.common.protocol.heartbeat.ConsumeType;
import org.apache.rocketmq.common.protocol.heartbeat.ConsumerData;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
import org.apache.rocketmq.common.protocol.heartbeat.SubscriptionData;
import org.apache.rocketmq.common.subscription.SubscriptionGroupConfig;
import org.apache.rocketmq.remoting.netty.FileRegionEncoder;
import org.apache.rocketmq.remoting.netty.NettyClientConfig;
import org.apache.rocketmq.remoting.netty.NettyServerConfig;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.store.GetMessageResult;
import org.apache.rocketmq.store.SelectMappedBufferResult;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.apache.rocketmq.store.logfile.MappedFile;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class BatchProtocolPullTest extends BatchProtocol {
    private BrokerConfig brokerConfig;
    private final List<String> topics = new ArrayList<>();
    private final String consumerGroup = "consumer-group";
    private final String producerGroup = "producer-group";
    private final int totalRequestNum = 20;
    private final Integer queue = 0;
    private final String topicPrefix = "batch-protocol-";
    private final Random random = new Random();

    @Before
    public void init() throws Exception {
        this.brokerConfig = new BrokerConfig();
        this.brokerController = null;
        this.brokerController = new BrokerController(
                this.brokerConfig,
                new NettyServerConfig(),
                new NettyClientConfig(),
                new MessageStoreConfig());
        assertThat(brokerController.initialize()).isTrue();
        brokerController.start();

        Channel mockChannel = mock(Channel.class);
        // when(mockChannel.isWritable()).thenReturn(true);
        when(mockChannel.remoteAddress()).thenReturn(new InetSocketAddress(1024));
        when(ctx.channel()).thenReturn(mockChannel);
        when(ctx.channel().isWritable()).thenReturn(true);
        ClientChannelInfo clientChannelInfo = new ClientChannelInfo(mockChannel);

        // prepare topics
        TopicConfigManager topicConfigManager = brokerController.getTopicConfigManager();
        for (int i = 0; i < totalRequestNum; i++) {
            String topic = topicPrefix + i;
            topicConfigManager.getTopicConfigTable().put(topic, new TopicConfig(topic));
            topics.add(topic);
        }

        // prepare subscribe group
        SubscriptionGroupManager subscriptionGroupManager = brokerController.getSubscriptionGroupManager();
        SubscriptionGroupConfig subscriptionGroupConfig = new SubscriptionGroupConfig();
        subscriptionGroupConfig.setGroupName(consumerGroup);
        subscriptionGroupManager.updateSubscriptionGroupConfig(subscriptionGroupConfig);

        ConsumerData consumerData = createConsumerData(consumerGroup, topics);
        brokerController.getConsumerManager().registerConsumer(
                consumerData.getGroupName(),
                clientChannelInfo,
                consumerData.getConsumeType(),
                consumerData.getMessageModel(),
                consumerData.getConsumeFromWhere(),
                consumerData.getSubscriptionDataSet(),
                false);
    }

    @After
    public void after() {
        brokerController.getMessageStore().destroy();
        brokerController.shutdown();
    }

    @Test
    public void testPullBatchProtocol() throws Exception {
        CommonBatchProcessor commonBatchProcessor = brokerController.getCommonBatchProcessor();

        // send some message to topics
        for (String topic : topics) {
            RemotingCommand sendRequest = createSendRequest(producerGroup, topic, queue);
            RemotingCommand sendResponse = brokerController.getSendProcessor().processRequest(ctx, sendRequest);
            assertThat(sendResponse.getCode()).isEqualTo(ResponseCode.SUCCESS);
        }
        await().atMost(5, SECONDS).until(fullyDispatched(this.brokerController.getMessageStore()));

        Map<Integer, RemotingCommand> expectedRequests = new HashMap<>();

        Long offset = 0L;
        for (String topic : topics) {
            RemotingCommand childPullRequest = createPullRequest(consumerGroup, topic, queue, offset);
            expectedRequests.put(childPullRequest.getOpaque(), childPullRequest);
        }

        RemotingCommand batchRequest = RemotingCommand.mergeChildren(new ArrayList<>(expectedRequests.values()));
        makeBatchRequestHeader(batchRequest, RequestCode.PULL_MESSAGE);

        // turn [zero-copy] off
        this.brokerConfig.setTransferMsgByHeap(true);
        CompletableFuture<RemotingCommand> batchFuture = commonBatchProcessor.asyncProcessRequest(ctx, batchRequest, callback);

        assertThat(batchFuture.isDone()).isTrue();
        RemotingCommand batchResponse = batchFuture.get();
        List<RemotingCommand> childResponses = RemotingCommand.parseChildren(batchResponse);
        assertThat(childResponses).hasSize(totalRequestNum);

        // assertion on responses.
        for (RemotingCommand actualChildResponse : childResponses) {
            int opaque = actualChildResponse.getOpaque();
            assertThat(expectedRequests).containsKey(opaque);
            assertThat(actualChildResponse.getBody()).isNotNull();

            PullMessageResponseHeader responseHeader =
                    (PullMessageResponseHeader) actualChildResponse.decodeCommandCustomHeader(PullMessageResponseHeader.class);

            ByteBuffer byteBuffer = ByteBuffer.wrap(actualChildResponse.getBody());
            List<MessageExt> msgList = MessageDecoder.decodes(byteBuffer);
        }
    }

    @Test
    public void testPullZeroCopyBatchProtocol() throws Exception {
        CommonBatchProcessor commonBatchProcessor = brokerController.getCommonBatchProcessor();

        // send some message to topics
        for (String topic : topics) {
            RemotingCommand sendRequest = createSendRequest(producerGroup, topic, queue);
            RemotingCommand sendResponse = brokerController.getSendProcessor().processRequest(ctx, sendRequest);
            assertThat(sendResponse.getCode()).isEqualTo(ResponseCode.SUCCESS);
        }

        await().atMost(5, SECONDS).until(fullyDispatched(this.brokerController.getMessageStore()));

        Map<Integer, RemotingCommand> expectedRequests = new HashMap<>();

        Long offset = 0L;
        for (String topic : topics) {
            RemotingCommand childPullRequest = createPullRequest(consumerGroup, topic, queue, offset);
            expectedRequests.put(childPullRequest.getOpaque(), childPullRequest);
        }

        RemotingCommand batchRequest = RemotingCommand.mergeChildren(new ArrayList<>(expectedRequests.values()));
        makeBatchRequestHeader(batchRequest, RequestCode.PULL_MESSAGE);

        // turn [zero-copy] on
        this.brokerConfig.setTransferMsgByHeap(false);
        CompletableFuture<RemotingCommand> batchFuture = commonBatchProcessor.asyncProcessRequest(ctx, batchRequest, callback);

        assertThat(batchFuture.isDone()).isTrue();
        RemotingCommand batchResponse = batchFuture.get();
        assertThat(batchResponse.getFileRegionAttachment()).isNotNull();
        assertThat(batchResponse.getFileRegionAttachment()).isInstanceOf(BatchManyMessageTransfer.class);

        BatchManyMessageTransfer fileRegion = (BatchManyMessageTransfer) batchResponse.getFileRegionAttachment();

        FileRegionEncoder fileRegionEncoder = new FileRegionEncoder();
        ByteBuf batchResponseBuf = Unpooled.buffer((int) fileRegion.count());

        fileRegionEncoder.encode(null, fileRegion, batchResponseBuf);

        // strip 4 bytes to simulate NettyDecoder.
        batchResponseBuf.readerIndex(4);
        RemotingCommand decodeBatchResponse = RemotingCommand.decode(batchResponseBuf);

        List<RemotingCommand> childResponses = RemotingCommand.parseChildren(decodeBatchResponse);
        assertThat(childResponses).hasSize(totalRequestNum);

        assertMmapExist(fileRegion, true);
        assertThat(batchResponse.getFinallyReleasingCallback()).isNotNull();
        // release mmap
        batchResponse.getFinallyReleasingCallback().run();
        assertMmapExist(fileRegion, false);

        for (RemotingCommand actualChildResponse : childResponses) {
            int opaque = actualChildResponse.getOpaque();
            assertThat(expectedRequests).containsKey(opaque);
            assertThat(actualChildResponse.getBody()).isNotNull();

            PullMessageResponseHeader responseHeader =
                    (PullMessageResponseHeader) actualChildResponse.decodeCommandCustomHeader(PullMessageResponseHeader.class);

            ByteBuffer byteBuffer = ByteBuffer.wrap(actualChildResponse.getBody());
            List<MessageExt> msgList = MessageDecoder.decodes(byteBuffer);
        }
    }

    @Test
    public void testPartialLongPollingBatchProtocol() throws Exception {
        CommonBatchProcessor commonBatchProcessor = brokerController.getCommonBatchProcessor();

        Map<Integer, RemotingCommand> childRequests = new HashMap<>();

        // make sure [longPollingTopic] won't get message.
        Long offset = 0L;
        Integer longPollingOpaque = null;
        for (String topic : topics) {
            RemotingCommand childPullRequest = createPullRequest(consumerGroup, topic, queue, offset);
            childRequests.put(childPullRequest.getOpaque(), childPullRequest);

            if (topic.endsWith("16")) {
                longPollingOpaque = childPullRequest.getOpaque();
                continue;
            }
            RemotingCommand sendRequest = createSendRequest(producerGroup, topic, queue);
            RemotingCommand sendResponse = brokerController.getSendProcessor().processRequest(ctx, sendRequest);
            assertThat(sendResponse.getCode()).isEqualTo(ResponseCode.SUCCESS);
        }

        await().atMost(5, SECONDS).until(fullyDispatched(this.brokerController.getMessageStore()));

        RemotingCommand batchRequest = RemotingCommand.mergeChildren(new ArrayList<>(childRequests.values()));
        makeBatchRequestHeader(batchRequest, RequestCode.PULL_MESSAGE);

        // turn [zero-copy] off
        this.brokerConfig.setTransferMsgByHeap(true);
        CompletableFuture<RemotingCommand> batchFuture = commonBatchProcessor.asyncProcessRequest(ctx, batchRequest, callback);

        assertThat(batchFuture.isDone()).isTrue();
        RemotingCommand batchResponse = batchFuture.get();
        List<RemotingCommand> childResponses = RemotingCommand.parseChildren(batchResponse);
        assertThat(childResponses).hasSize(totalRequestNum);

        // assertion on responses.
        for (RemotingCommand actualChildResponse : childResponses) {
            int opaque = actualChildResponse.getOpaque();
            assertThat(childRequests).containsKey(opaque);

            if (Objects.equals(longPollingOpaque, opaque)) {
                assertThat(actualChildResponse.getCode()).isEqualTo(ResponseCode.PULL_NOT_FOUND);
                assertThat(actualChildResponse.getRemark()).isEqualTo(MergeBatchResponseStrategy.REMARK_PULL_NOT_FOUND);
            } else {
                assertThat(actualChildResponse.getCode()).isEqualTo(ResponseCode.SUCCESS);
            }
        }
    }

    @Test
    public void testPullPartialZeroCopyBatchProtocol() throws Exception {
        CommonBatchProcessor commonBatchProcessor = brokerController.getCommonBatchProcessor();

        Map<Integer, RemotingCommand> childRequests = new HashMap<>();

        // make sure [longPollingTopic] won't get message.
        Long offset = 0L;
        Integer longPollingOpaque = null;
        for (String topic : topics) {
            RemotingCommand childPullRequest = createPullRequest(consumerGroup, topic, queue, offset);
            childRequests.put(childPullRequest.getOpaque(), childPullRequest);

            if (topic.endsWith("16")) {
                longPollingOpaque = childPullRequest.getOpaque();
                continue;
            }
            RemotingCommand sendRequest = createSendRequest(producerGroup, topic, queue);
            RemotingCommand sendResponse = brokerController.getSendProcessor().processRequest(ctx, sendRequest);
            assertThat(sendResponse.getCode()).isEqualTo(ResponseCode.SUCCESS);
        }

        await().atMost(5, SECONDS).until(fullyDispatched(this.brokerController.getMessageStore()));

        RemotingCommand batchRequest = RemotingCommand.mergeChildren(new ArrayList<>(childRequests.values()));
        makeBatchRequestHeader(batchRequest, RequestCode.PULL_MESSAGE);

        // turn [zero-copy] on
        this.brokerConfig.setTransferMsgByHeap(false);
        CompletableFuture<RemotingCommand> batchFuture = commonBatchProcessor.asyncProcessRequest(ctx, batchRequest, callback);

        assertThat(batchFuture.isDone()).isTrue();
        RemotingCommand batchResponse = batchFuture.get();
        assertThat(batchResponse.getFileRegionAttachment()).isNotNull();
        assertThat(batchResponse.getFileRegionAttachment()).isInstanceOf(BatchManyMessageTransfer.class);

        BatchManyMessageTransfer fileRegion = (BatchManyMessageTransfer) batchResponse.getFileRegionAttachment();

        FileRegionEncoder fileRegionEncoder = new FileRegionEncoder();
        ByteBuf batchResponseBuf = Unpooled.buffer((int) fileRegion.count());

        fileRegionEncoder.encode(null, fileRegion, batchResponseBuf);

        // strip 4 bytes to simulate NettyDecoder.
        batchResponseBuf.readerIndex(4);
        RemotingCommand decodeBatchResponse = RemotingCommand.decode(batchResponseBuf);

        List<RemotingCommand> childResponses = RemotingCommand.parseChildren(decodeBatchResponse);
        assertThat(childResponses).hasSize(totalRequestNum - 1);

        assertMmapExist(fileRegion, true);
        assertThat(batchResponse.getFinallyReleasingCallback()).isNotNull();
        // release mmap
        batchResponse.getFinallyReleasingCallback().run();
        assertMmapExist(fileRegion, false);

        // assertion on responses.
        for (RemotingCommand actualChildResponse : childResponses) {
            int opaque = actualChildResponse.getOpaque();
            assertThat(childRequests).containsKey(opaque);
            assertThat(opaque).isNotEqualTo(longPollingOpaque);
            assertThat(actualChildResponse.getCode()).isEqualTo(ResponseCode.SUCCESS);
        }
    }

    @Test
    public void testAllLongPollingBatchProtocol() throws Exception {
        CommonBatchProcessor commonBatchProcessor = brokerController.getCommonBatchProcessor();

        Map<Integer, RemotingCommand> childRequests = new HashMap<>();
        Map<Integer, String> opaqueToTopic = new HashMap<>();

        Long offset = 0L;
        for (String topic : topics) {
            RemotingCommand childPullRequest = createPullRequest(consumerGroup, topic, queue, offset);
            childRequests.put(childPullRequest.getOpaque(), childPullRequest);
            opaqueToTopic.put(childPullRequest.getOpaque(), topic);
        }

        RemotingCommand batchRequest = RemotingCommand.mergeChildren(new ArrayList<>(childRequests.values()));
        makeBatchRequestHeader(batchRequest, RequestCode.PULL_MESSAGE);

        // turn [zero-copy] off
        this.brokerConfig.setTransferMsgByHeap(true);
        CompletableFuture<RemotingCommand> batchFuture = commonBatchProcessor.asyncProcessRequest(ctx, batchRequest, callback);

        assertThat(batchFuture.isDone()).isFalse();

        // send a message trying to wake up batch-long-polling.
        String sendDataToRandomTopic = topicPrefix + random.nextInt(totalRequestNum);
        RemotingCommand sendRequest = createSendRequest(producerGroup, sendDataToRandomTopic, queue);
        RemotingCommand sendResponse = brokerController.getSendProcessor().processRequest(ctx, sendRequest);
        assertThat(sendResponse.getCode()).isEqualTo(ResponseCode.SUCCESS);

        await().atMost(5, SECONDS).until(fullyDispatched(this.brokerController.getMessageStore()));
        assertThat(batchFuture.isDone()).isTrue();

        RemotingCommand batchResponse = batchFuture.get();
        List<RemotingCommand> childResponses = RemotingCommand.parseChildren(batchResponse);
        assertThat(childResponses).hasSize(totalRequestNum);

        // assertion on responses.
        for (RemotingCommand actualChildResponse : childResponses) {
            int opaque = actualChildResponse.getOpaque();
            assertThat(childRequests).containsKey(opaque);

            if (Objects.equals(opaqueToTopic.get(opaque), sendDataToRandomTopic)) {
                // has data
                assertThat(actualChildResponse.getCode()).isEqualTo(ResponseCode.SUCCESS);
            } else {
                // has no data
                assertThat(actualChildResponse.getCode()).isEqualTo(ResponseCode.PULL_NOT_FOUND);
            }
        }
    }

    static ConsumerData createConsumerData(String group, List<String> topics) {
        ConsumerData consumerData = new ConsumerData();
        consumerData.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        consumerData.setConsumeType(ConsumeType.CONSUME_PASSIVELY);
        consumerData.setGroupName(group);
        consumerData.setMessageModel(MessageModel.CLUSTERING);
        Set<SubscriptionData> subscriptionDataSet = new HashSet<>();
        for (String topic : topics) {
            SubscriptionData subscriptionData = new SubscriptionData();
            subscriptionData.setTopic(topic);
            subscriptionData.setSubString("*");
            subscriptionData.setSubVersion(100L);
            subscriptionDataSet.add(subscriptionData);
        }

        consumerData.setSubscriptionDataSet(subscriptionDataSet);
        return consumerData;
    }

    private void assertMmapExist(BatchManyMessageTransfer fileRegion, boolean expectHasMmap) throws NoSuchFieldException, IllegalAccessException {
        Field manyMessageTransferListField = BatchManyMessageTransfer.class.getDeclaredField("manyMessageTransferList");
        manyMessageTransferListField.setAccessible(true);

        boolean hasMmap = hasMmap(fileRegion, (List<ManyMessageTransfer>) manyMessageTransferListField.get(fileRegion));

        assertThat(hasMmap).isEqualTo(expectHasMmap);
    }

    private boolean hasMmap(BatchManyMessageTransfer fileRegion, List<ManyMessageTransfer> manyMessageTransferList) throws IllegalAccessException, NoSuchFieldException {
        for (ManyMessageTransfer manyMessageTransfer : manyMessageTransferList) {
            Field getMessageResultField = ManyMessageTransfer.class.getDeclaredField("getMessageResult");
            getMessageResultField.setAccessible(true);
            GetMessageResult getMessageResult = (GetMessageResult) getMessageResultField.get(manyMessageTransfer);
            for (SelectMappedBufferResult selectMappedBufferResult : getMessageResult.getMessageMapedList()) {
                Field mappedFileField = SelectMappedBufferResult.class.getDeclaredField("mappedFile");
                mappedFileField.setAccessible(true);
                MappedFile mappedFile = (MappedFile) mappedFileField.get(selectMappedBufferResult);
                if (mappedFile != null) {
                    return true;
                }
            }
        }
        return false;
    }
}
