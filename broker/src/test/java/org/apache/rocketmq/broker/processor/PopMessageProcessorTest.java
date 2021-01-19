package org.apache.rocketmq.broker.processor;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Set;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.client.ClientChannelInfo;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.constant.ConsumeInitMode;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.protocol.RequestCode;
import org.apache.rocketmq.common.protocol.ResponseCode;
import org.apache.rocketmq.common.protocol.header.PopMessageRequestHeader;
import org.apache.rocketmq.common.protocol.heartbeat.ConsumeType;
import org.apache.rocketmq.common.protocol.heartbeat.ConsumerData;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
import org.apache.rocketmq.common.protocol.heartbeat.SubscriptionData;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.netty.NettyClientConfig;
import org.apache.rocketmq.remoting.netty.NettyServerConfig;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.store.AppendMessageResult;
import org.apache.rocketmq.store.AppendMessageStatus;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.GetMessageResult;
import org.apache.rocketmq.store.GetMessageStatus;
import org.apache.rocketmq.store.MappedFile;
import org.apache.rocketmq.store.PutMessageResult;
import org.apache.rocketmq.store.PutMessageStatus;
import org.apache.rocketmq.store.SelectMappedBufferResult;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.apache.rocketmq.store.schedule.ScheduleMessageService;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.MockitoJUnitRunner;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class PopMessageProcessorTest {
    private PopMessageProcessor popMessageProcessor;

    @Spy
    private BrokerController brokerController = new BrokerController(new BrokerConfig(), new NettyServerConfig(), new NettyClientConfig(), new MessageStoreConfig());
    @Mock
    private ChannelHandlerContext handlerContext;
    @Mock
    private DefaultMessageStore messageStore;
    private ScheduleMessageService scheduleMessageService;
    private ClientChannelInfo clientChannelInfo;
    private String group = "FooBarGroup";
    private String topic = "FooBar";

    @Before
    public void init() {
        brokerController.setMessageStore(messageStore);
        popMessageProcessor = new PopMessageProcessor(brokerController);
        scheduleMessageService = new ScheduleMessageService(messageStore);
        MessageStoreConfig messageStoreConfig = new MessageStoreConfig();
        messageStoreConfig.setMessageDelayLevel("5s 10s");
        when(messageStore.getMessageStoreConfig()).thenReturn(messageStoreConfig);
        scheduleMessageService.parseDelayLevel();
        when(messageStore.getScheduleMessageService()).thenReturn(scheduleMessageService);
        when(messageStore.putMessage(any())).thenReturn(new PutMessageResult(PutMessageStatus.PUT_OK, new AppendMessageResult(AppendMessageStatus.PUT_OK)));
        Channel mockChannel = mock(Channel.class);
        when(mockChannel.remoteAddress()).thenReturn(new InetSocketAddress(1024));
        when(handlerContext.channel()).thenReturn(mockChannel);
        brokerController.getTopicConfigManager().getTopicConfigTable().put(topic, new TopicConfig());
        clientChannelInfo = new ClientChannelInfo(mockChannel);
        ConsumerData consumerData = createConsumerData(group, topic);
        brokerController.getConsumerManager().registerConsumer(
            consumerData.getGroupName(),
            clientChannelInfo,
            consumerData.getConsumeType(),
            consumerData.getMessageModel(),
            consumerData.getConsumeFromWhere(),
            consumerData.getSubscriptionDataSet(),
            false);
    }

    @Test
    public void testProcessRequest_TopicNotExist() throws RemotingCommandException {
        brokerController.getTopicConfigManager().getTopicConfigTable().remove(topic);
        final RemotingCommand request = createPopMsgCommand();
        RemotingCommand response = popMessageProcessor.processRequest(handlerContext, request);
        assertThat(response).isNotNull();
        assertThat(response.getCode()).isEqualTo(ResponseCode.TOPIC_NOT_EXIST);
        assertThat(response.getRemark()).contains("topic[" + topic + "] not exist");
    }

    @Test
    public void testProcessRequest_SubNotExist() throws RemotingCommandException {
        brokerController.getConsumerManager().unregisterConsumer(group, clientChannelInfo, false);
        final RemotingCommand request = createPopMsgCommand();
        RemotingCommand response = popMessageProcessor.processRequest(handlerContext, request);
        assertThat(response).isNotNull();
        assertThat(response.getCode()).isEqualTo(ResponseCode.SUBSCRIPTION_NOT_EXIST);
        assertThat(response.getRemark()).contains("consumer's group info not exist");
    }

    @Test
    public void testProcessRequest_Found() throws RemotingCommandException {
        GetMessageResult getMessageResult = createGetMessageResult(1);
        when(messageStore.getMessage(anyString(), anyString(), anyInt(), anyLong(), anyInt(), any())).thenReturn(getMessageResult);

        final RemotingCommand request = createPopMsgCommand();
        RemotingCommand response = popMessageProcessor.processRequest(handlerContext, request);
        assertThat(response).isNotNull();
        assertThat(response.getCode()).isEqualTo(ResponseCode.SUCCESS);
    }

    @Test
    public void testProcessRequest_MsgWasRemoving() throws RemotingCommandException {
        GetMessageResult getMessageResult = createGetMessageResult(1);
        getMessageResult.setStatus(GetMessageStatus.MESSAGE_WAS_REMOVING);
        when(messageStore.getMessage(anyString(), anyString(), anyInt(), anyLong(), anyInt(), any())).thenReturn(getMessageResult);

        final RemotingCommand request = createPopMsgCommand();
        RemotingCommand response = popMessageProcessor.processRequest(handlerContext, request);
        assertThat(response).isNotNull();
        assertThat(response.getCode()).isEqualTo(ResponseCode.SUCCESS);
    }

    @Test
    public void testProcessRequest_NoMsgInQueue() throws RemotingCommandException {
        GetMessageResult getMessageResult = createGetMessageResult(0);
        getMessageResult.setStatus(GetMessageStatus.NO_MESSAGE_IN_QUEUE);
        when(messageStore.getMessage(anyString(), anyString(), anyInt(), anyLong(), anyInt(), any())).thenReturn(getMessageResult);

        final RemotingCommand request = createPopMsgCommand();
        RemotingCommand response = popMessageProcessor.processRequest(handlerContext, request);
        assertThat(response).isNull();
    }


    private RemotingCommand createPopMsgCommand() {
        PopMessageRequestHeader requestHeader = new PopMessageRequestHeader();
        requestHeader.setConsumerGroup(group);
        requestHeader.setMaxMsgNums(30);
        requestHeader.setQueueId(-1);
        requestHeader.setTopic(topic);
        requestHeader.setInvisibleTime(10_000);
        requestHeader.setInitMode(ConsumeInitMode.MAX);
        requestHeader.setOrder(false);
        requestHeader.setPollTime(15_000);
        requestHeader.setBornTime(System.currentTimeMillis());
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.POP_MESSAGE, requestHeader);
        request.makeCustomHeaderToNet();
        return request;
    }


    static ConsumerData createConsumerData(String group, String topic) {
        ConsumerData consumerData = new ConsumerData();
        consumerData.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        consumerData.setConsumeType(ConsumeType.CONSUME_PASSIVELY);
        consumerData.setGroupName(group);
        consumerData.setMessageModel(MessageModel.CLUSTERING);
        Set<SubscriptionData> subscriptionDataSet = new HashSet<>();
        SubscriptionData subscriptionData = new SubscriptionData();
        subscriptionData.setTopic(topic);
        subscriptionData.setSubString("*");
        subscriptionData.setSubVersion(100L);
        subscriptionDataSet.add(subscriptionData);
        consumerData.setSubscriptionDataSet(subscriptionDataSet);
        return consumerData;
    }

    private GetMessageResult createGetMessageResult(int msgCnt) {
        GetMessageResult getMessageResult = new GetMessageResult();
        getMessageResult.setStatus(GetMessageStatus.FOUND);
        getMessageResult.setMinOffset(100);
        getMessageResult.setMaxOffset(1024);
        getMessageResult.setNextBeginOffset(516);
        for (int i = 0; i < msgCnt; i++) {
            ByteBuffer bb = ByteBuffer.allocate(64);
            bb.putLong(MessageDecoder.MESSAGE_STORE_TIMESTAMP_POSITION, System.currentTimeMillis());
            getMessageResult.addMessage(new SelectMappedBufferResult(200, bb, 64, new MappedFile()));
        }
        return getMessageResult;
    }
}