package org.apache.rocketmq.test.proxy;

import apache.rocketmq.v1.AckMessageResponse;
import apache.rocketmq.v1.Address;
import apache.rocketmq.v1.AddressScheme;
import apache.rocketmq.v1.Endpoints;
import apache.rocketmq.v1.MessagingServiceGrpc;
import apache.rocketmq.v1.QueryRouteResponse;
import apache.rocketmq.v1.ReceiveMessageResponse;
import apache.rocketmq.v1.SendMessageResponse;
import com.google.common.base.Stopwatch;
import io.grpc.Channel;
import java.net.URL;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.proxy.config.ConfigurationManager;
import org.apache.rocketmq.proxy.grpc.v1.GrpcMessagingProcessor;
import org.apache.rocketmq.proxy.grpc.v1.service.ClusterGrpcService;
import org.apache.rocketmq.proxy.grpc.v1.service.GrpcForwardService;
import org.apache.rocketmq.test.base.GrpcBaseTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.apache.rocketmq.common.message.MessageClientIDSetter.createUniqID;
import static org.apache.rocketmq.proxy.config.ConfigurationManager.RMQ_PROXY_HOME;
import static org.junit.Assert.assertTrue;

public class ClusterGrpcTest extends GrpcBaseTest {

    private final int PORT = 8083;
    private GrpcForwardService grpcForwardService;
    private MessagingServiceGrpc.MessagingServiceBlockingStub blockingStub;

    @Before
    public void setUp() throws Exception {
        String mockProxyHome = "/mock/rmq/proxy/home";
        URL mockProxyHomeURL = getClass().getClassLoader().getResource("rmq-proxy-home");
        if (mockProxyHomeURL != null) {
            mockProxyHome = mockProxyHomeURL.toURI().getPath();
        }
        System.setProperty(RMQ_PROXY_HOME, mockProxyHome);
        ConfigurationManager.initEnv();
        ConfigurationManager.intConfig();
        ConfigurationManager.getProxyConfig().setGrpcServerPort(PORT);
        ConfigurationManager.getProxyConfig().setNameSrvAddr(nsAddr);
        grpcForwardService = new ClusterGrpcService();
        grpcForwardService.start();
        GrpcMessagingProcessor processor = new GrpcMessagingProcessor(grpcForwardService);
        Channel channel = setUpServer(processor, ConfigurationManager.getProxyConfig().getGrpcServerPort(), true);
        blockingStub = MessagingServiceGrpc.newBlockingStub(channel);
    }

    @After
    public void tearDown() throws Exception {
        grpcForwardService.shutdown();
        shutdown();
    }

    @Test
    public void testQueryRoute() {
        String topic = initTopic();
        QueryRouteResponse response = blockingStub.queryRoute(buildQueryRouteRequest(topic, Endpoints.newBuilder()
            .setScheme(AddressScheme.IPv4)
            .addAddresses(Address.newBuilder()
                .setHost("127.0.0.1")
                .setPort(PORT)
                .build())
            .build()));
        assertQueryRoute(response, brokerControllerList.size());
    }

    @Test
    public void testSendReceiveMessage() {
        String group = "group";
        String messageId = createUniqID();
        SendMessageResponse sendResponse = blockingStub.sendMessage(buildSendMessageRequest(broker1Name, messageId));
        assertSendMessage(sendResponse, messageId);

        ReceiveMessageResponse receiveResponse = blockingStub.withDeadlineAfter(3, TimeUnit.SECONDS)
            .receiveMessage(buildReceiveMessageRequest(group, broker1Name));
        assertReceiveMessage(receiveResponse, messageId);
        String receiptHandle = receiveResponse.getMessages(0).getSystemAttribute().getReceiptHandle();
        AckMessageResponse ackMessageResponse = blockingStub.ackMessage(buildAckMessageRequest(group, broker1Name, receiptHandle));
        assertAck(ackMessageResponse);
    }

    @Test
    public void testSendReceiveDelayMessage() {
        String group = "group";
        String messageId = createUniqID();
        SendMessageResponse sendResponse = blockingStub.sendMessage(buildSendDelayMessageRequest(broker1Name, messageId, 2));
        assertSendMessage(sendResponse, messageId);

        Stopwatch stopwatch = Stopwatch.createStarted();
        ReceiveMessageResponse receiveResponse = blockingStub.withDeadlineAfter(10, TimeUnit.SECONDS)
            .receiveMessage(buildReceiveMessageRequest(group, broker1Name));
        long rcvTime = stopwatch.elapsed(TimeUnit.SECONDS);
        assertTrue(Math.abs(rcvTime - 5) < 2);

        assertReceiveMessage(receiveResponse, messageId);
        String receiptHandle = receiveResponse.getMessages(0).getSystemAttribute().getReceiptHandle();
        AckMessageResponse ackMessageResponse = blockingStub.ackMessage(buildAckMessageRequest(group, broker1Name, receiptHandle));
        assertAck(ackMessageResponse);
    }


}
