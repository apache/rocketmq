package org.apache.rocketmq.broker.processor;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.client.ClientRole;
import org.apache.rocketmq.common.client.Subscription;
import org.apache.rocketmq.common.protocol.RequestCode;
import org.apache.rocketmq.common.protocol.header.mqtt.*;
import org.apache.rocketmq.common.protocol.heartbeat.MqttSubscriptionData;
import org.apache.rocketmq.mqtt.client.MQTTSession;
import org.apache.rocketmq.mqtt.constant.MqttConstant;
import org.apache.rocketmq.remoting.ClientConfig;
import org.apache.rocketmq.remoting.RemotingChannel;
import org.apache.rocketmq.remoting.ServerConfig;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.netty.CodecHelper;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.store.DefaultMQTTInfoStore;
import org.apache.rocketmq.store.MQTTInfoStore;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.MockitoJUnitRunner;

import java.net.InetSocketAddress;
import java.util.HashSet;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;


@RunWith(MockitoJUnitRunner.class)
public class MQTTProcessorTest {


    private MQTTProcessor requestProcessor;
    @Spy
    private BrokerController brokerController = new BrokerController(new BrokerConfig(), new ServerConfig(), new ClientConfig(), new MessageStoreConfig());

    private static MQTTInfoStore mqttInfoStore;
    @Mock
    private RemotingChannel remotingChannel;

    private MQTTSession mqttSession;
    private static final Gson GSON = new Gson();
    private static final String CLIENT_ID = "testClient";
    private static final String ROOT_TOPIC = "rootTopic";


    private Subscription subscription;

    static {

        try {
            mqttInfoStore = new DefaultMQTTInfoStore();
            mqttInfoStore.load();
            mqttInfoStore.start();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    @Before
    public void init() throws Exception {

        when(brokerController.getMqttInfoStore()).thenReturn(mqttInfoStore);
        when(remotingChannel.remoteAddress()).thenReturn(new InetSocketAddress(1024));
        requestProcessor = new MQTTProcessor(brokerController);
        this.mqttSession = new MQTTSession(CLIENT_ID, ClientRole.IOTCLIENT, new HashSet<String>() {
            {
                add("IOT_GROUP");
            }
        }, true, true, null, System.currentTimeMillis(), null);


        subscription = new Subscription();
        subscription.setCleanSession(true);
        subscription.getSubscriptionTable().put("topic/a", new MqttSubscriptionData(1, "client1", "topic/a"));
        subscription.getSubscriptionTable().put("topic/+", new MqttSubscriptionData(1, "client1", "topic/+"));
        subscription.setLastUpdateTimestamp(System.currentTimeMillis());
    }

    @Test
    public void testReqProc_IsClient2SubscriptionPersisted() throws RemotingCommandException {
        mqttInfoStore.putData(CLIENT_ID + MqttConstant.PERSIST_CLIENT_SUFFIX, GSON.toJson(mqttSession));
        final RemotingCommand request = createIsClient2SubscriptionPersistedRequestHeader(RequestCode.MQTT_IS_CLIENT2SUBSCRIPTION_PERSISTED);
        RemotingCommand responseToReturn = requestProcessor.processRequest(remotingChannel, request);
        if (responseToReturn != null) {
            IsClient2SubscriptionPersistedResponseHeader mqttHeader = (IsClient2SubscriptionPersistedResponseHeader) responseToReturn.readCustomHeader();
            assertThat(mqttHeader).isNotNull();
            assertThat(mqttHeader.isPersisted()).isEqualTo(true);
        }
    }


    private RemotingCommand createIsClient2SubscriptionPersistedRequestHeader(int requestCode) {
        IsClient2SubscriptionPersistedRequestHeader requestHeader = new IsClient2SubscriptionPersistedRequestHeader();
        requestHeader.setClientId(CLIENT_ID);
        requestHeader.setCleanSession(false);
        RemotingCommand request = RemotingCommand.createRequestCommand(requestCode, requestHeader);
        request.setBody(new byte[]{'a'});
        CodecHelper.makeCustomHeaderToNet(request);
        return request;
    }





    @Test
    public void testReqProc_AddorUpdateRootTopic2Clients() throws RemotingCommandException {
        final RemotingCommand request = createAddorUpdateRootTopic2Clients(RequestCode.MQTT_ADD_OR_UPDATE_ROOTTOPIC2CLIENTS);
        RemotingCommand responseToReturn = requestProcessor.processRequest(remotingChannel, request);
        if (responseToReturn != null) {
            AddOrUpdateRootTopic2ClientsResponseHeader mqttHeader = (AddOrUpdateRootTopic2ClientsResponseHeader) responseToReturn.readCustomHeader();
            assertThat(mqttHeader).isNotNull();
            assertThat(mqttHeader.isOperationSuccess()).isEqualTo(true);
        }
        String value = mqttInfoStore.getValue(ROOT_TOPIC);
        Set<String> clientsId;
        if (value != null) {
            clientsId = GSON.fromJson(value, new TypeToken<Set<String>>() {
            }.getType());
        } else {
            clientsId = new HashSet<>();
        }
        assertThat(clientsId).contains(CLIENT_ID);
    }


    private RemotingCommand createAddorUpdateRootTopic2Clients(int requestCode) {
        AddOrUpdateRootTopic2ClientsRequestHeader requestHeader = new AddOrUpdateRootTopic2ClientsRequestHeader();
        requestHeader.setRootTopic(ROOT_TOPIC);
        requestHeader.setClientId(CLIENT_ID);

        RemotingCommand request = RemotingCommand.createRequestCommand(requestCode, requestHeader);
        request.setBody(new byte[]
                {
                        'a'
                });
        CodecHelper.makeCustomHeaderToNet(request);
        return request;
    }


    @Test
    public void testReqProc_GetSubscriptionByClientId() throws RemotingCommandException {
        String subscriptionStr = GSON.toJson(subscription);
        this.mqttInfoStore.putData(CLIENT_ID + MqttConstant.PERSIST_SUBSCRIPTION_SUFFIX, subscriptionStr);

        final RemotingCommand request = createGetSubscriptionByClientId(RequestCode.MQTT_GET_SUBSCRIPTION_BY_CLIENT_ID);
        RemotingCommand responseToReturn = requestProcessor.processRequest(remotingChannel, request);
        if (responseToReturn != null) {
            GetSubscriptionByClientIdResponseHeader mqttHeader = (GetSubscriptionByClientIdResponseHeader) responseToReturn.readCustomHeader();
            assertThat(mqttHeader).isNotNull();
            assertThat(mqttHeader.getSubscription()).isNotNull();

            assertThat(subscription.getLastUpdateTimestamp()).isEqualTo(mqttHeader.getSubscription().getLastUpdateTimestamp());
        }


    }


    private RemotingCommand createGetSubscriptionByClientId(int requestCode) {
        GetSubscriptionByClientIdRequestHeader requestHeader = new GetSubscriptionByClientIdRequestHeader();
        requestHeader.setClientId(CLIENT_ID);

        RemotingCommand request = RemotingCommand.createRequestCommand(requestCode, requestHeader);
        request.setBody(new byte[]
                {
                        'a'
                });
        CodecHelper.makeCustomHeaderToNet(request);
        return request;
    }


}
