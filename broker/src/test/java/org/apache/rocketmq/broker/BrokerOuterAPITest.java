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

package org.apache.rocketmq.broker;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import io.netty.channel.DefaultChannelPromise;
import io.netty.util.concurrent.DefaultEventExecutor;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.rocketmq.auth.config.AuthConfig;
import org.apache.rocketmq.broker.out.BrokerOuterAPI;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.client.consumer.PullStatus;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.BrokerIdentity;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.remoting.common.SemaphoreReleaseOnlyOnce;
import org.apache.rocketmq.remoting.exception.RemotingTimeoutException;
import org.apache.rocketmq.remoting.netty.NettyClientConfig;
import org.apache.rocketmq.remoting.netty.NettyRemotingClient;
import org.apache.rocketmq.remoting.netty.NettyServerConfig;
import org.apache.rocketmq.remoting.netty.ResponseFuture;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;
import org.apache.rocketmq.remoting.protocol.RequestCode;
import org.apache.rocketmq.remoting.protocol.ResponseCode;
import org.apache.rocketmq.remoting.protocol.body.ClusterInfo;
import org.apache.rocketmq.remoting.protocol.body.TopicConfigSerializeWrapper;
import org.apache.rocketmq.remoting.protocol.header.PullMessageResponseHeader;
import org.apache.rocketmq.remoting.protocol.header.namesrv.QueryDataVersionResponseHeader;
import org.apache.rocketmq.remoting.protocol.header.namesrv.RegisterBrokerResponseHeader;
import org.apache.rocketmq.remoting.protocol.namesrv.RegisterBrokerResult;
import org.apache.rocketmq.remoting.protocol.route.BrokerData;
import org.apache.rocketmq.store.MessageStore;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.AdditionalMatchers.or;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.when;

@RunWith(PowerMockRunner.class)
@PrepareForTest(NettyRemotingClient.class)
public class BrokerOuterAPITest {
    @Mock
    private ChannelHandlerContext handlerContext;
    @Spy
    private BrokerController brokerController = new BrokerController(new BrokerConfig(), new NettyServerConfig(), new NettyClientConfig(), new MessageStoreConfig());
    @Mock
    private MessageStore messageStore;
    private String clusterName = "clusterName";
    private String brokerName = "brokerName";
    private String brokerAddr = "brokerAddr";
    private long brokerId = 0L;
    private String nameserver1 = "127.0.0.1";
    private String nameserver2 = "127.0.0.2";
    private String nameserver3 = "127.0.0.3";
    private int timeOut = 3000;

    @Mock
    private NettyRemotingClient nettyRemotingClient;

    private BrokerOuterAPI brokerOuterAPI;

    public void init() throws Exception {
        brokerOuterAPI = new BrokerOuterAPI(new NettyClientConfig(), new AuthConfig());
        Field field = BrokerOuterAPI.class.getDeclaredField("remotingClient");
        field.setAccessible(true);
        field.set(brokerOuterAPI, nettyRemotingClient);
    }

    @Test
    public void test_needRegister_normal() throws Exception {
        init();
        brokerOuterAPI.start();
        final RemotingCommand response = buildResponse(Boolean.TRUE);

        TopicConfigSerializeWrapper topicConfigSerializeWrapper = new TopicConfigSerializeWrapper();

        when(nettyRemotingClient.getNameServerAddressList()).thenReturn(Lists.asList(nameserver1, nameserver2, new String[] {nameserver3}));
        when(nettyRemotingClient.invokeSync(anyString(), any(RemotingCommand.class), anyLong())).thenReturn(response);
        List<Boolean> booleanList = brokerOuterAPI.needRegister(clusterName, brokerAddr, brokerName, brokerId, topicConfigSerializeWrapper, timeOut, false);
        assertTrue(booleanList.size() > 0);
        assertFalse(booleanList.contains(Boolean.FALSE));
    }

    @Test
    public void test_needRegister_timeout() throws Exception {
        init();
        brokerOuterAPI.start();

        TopicConfigSerializeWrapper topicConfigSerializeWrapper = new TopicConfigSerializeWrapper();

        when(nettyRemotingClient.getNameServerAddressList()).thenReturn(Lists.asList(nameserver1, nameserver2, new String[] {nameserver3}));

        when(nettyRemotingClient.invokeSync(anyString(), any(RemotingCommand.class), anyLong())).thenAnswer(new Answer<RemotingCommand>() {
            @Override
            public RemotingCommand answer(InvocationOnMock invocation) throws Throwable {
                if (invocation.getArgument(0) == nameserver1) {
                    return buildResponse(Boolean.TRUE);
                } else if (invocation.getArgument(0) == nameserver2) {
                    return buildResponse(Boolean.FALSE);
                } else if (invocation.getArgument(0) == nameserver3) {
                    TimeUnit.MILLISECONDS.sleep(timeOut + 20);
                    return buildResponse(Boolean.TRUE);
                }
                return buildResponse(Boolean.TRUE);
            }
        });
        List<Boolean> booleanList = brokerOuterAPI.needRegister(clusterName, brokerAddr, brokerName, brokerId, topicConfigSerializeWrapper, timeOut, false);
        assertEquals(2, booleanList.size());
        boolean success = Iterables.any(booleanList,
            new Predicate<Boolean>() {
                public boolean apply(Boolean input) {
                    return input;
                }
            });

        assertTrue(success);

    }

    @Test
    public void test_register_normal() throws Exception {
        init();
        brokerOuterAPI.start();

        final RemotingCommand response = RemotingCommand.createResponseCommand(RegisterBrokerResponseHeader.class);
        final RegisterBrokerResponseHeader responseHeader = (RegisterBrokerResponseHeader) response.readCustomHeader();
        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);

        TopicConfigSerializeWrapper topicConfigSerializeWrapper = new TopicConfigSerializeWrapper();

        when(nettyRemotingClient.getAvailableNameSrvList()).thenReturn(Lists.asList(nameserver1, nameserver2, new String[] {nameserver3}));
        when(nettyRemotingClient.invokeSync(anyString(), any(RemotingCommand.class), anyLong())).then(new Answer<RemotingCommand>() {
            @Override
            public RemotingCommand answer(InvocationOnMock mock) throws Throwable {
                RemotingCommand request = mock.getArgument(1);
                return response;
            }
        });
        List<RegisterBrokerResult> registerBrokerResultList = brokerOuterAPI.registerBrokerAll(clusterName, brokerAddr,
            brokerName,
            brokerId,
            "hasServerAddr",
            topicConfigSerializeWrapper,
            Lists.newArrayList(),
            false,
            timeOut,
            false,
            true,
            new BrokerIdentity());

        assertTrue(registerBrokerResultList.size() > 0);
    }

    @Test
    public void test_register_timeout() throws Exception {
        init();
        brokerOuterAPI.start();

        final RemotingCommand response = RemotingCommand.createResponseCommand(RegisterBrokerResponseHeader.class);
        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);

        TopicConfigSerializeWrapper topicConfigSerializeWrapper = new TopicConfigSerializeWrapper();

        when(nettyRemotingClient.getAvailableNameSrvList()).thenReturn(Lists.asList(nameserver1, nameserver2, new String[] {nameserver3}));
        final ArgumentCaptor<Long> timeoutMillisCaptor = ArgumentCaptor.forClass(Long.class);
        when(nettyRemotingClient.invokeSync(or(ArgumentMatchers.eq(nameserver1), ArgumentMatchers.eq(nameserver2)), any(RemotingCommand.class),
            timeoutMillisCaptor.capture())).thenReturn(response);
        when(nettyRemotingClient.invokeSync(ArgumentMatchers.eq(nameserver3), any(RemotingCommand.class), anyLong())).thenThrow(RemotingTimeoutException.class);
        List<RegisterBrokerResult> registerBrokerResultList = brokerOuterAPI.registerBrokerAll(clusterName, brokerAddr, brokerName, brokerId, "hasServerAddr", topicConfigSerializeWrapper, Lists.<String>newArrayList(), false, timeOut, false, true, new BrokerIdentity());

        assertEquals(2, registerBrokerResultList.size());
    }

    @Test
    public void testGetBrokerClusterInfo() throws Exception {
        init();
        brokerOuterAPI.start();

        final RemotingCommand response = RemotingCommand.createResponseCommand(null);
        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);

        ClusterInfo want = new ClusterInfo();
        want.setBrokerAddrTable(new HashMap<>(Collections.singletonMap("key", new BrokerData("cluster", "broker", new HashMap<>(Collections.singletonMap(MixAll.MASTER_ID, "127.0.0.1:10911"))))));
        response.setBody(RemotingSerializable.encode(want));

        when(nettyRemotingClient.invokeSync(isNull(), argThat(argument -> argument.getCode() == RequestCode.GET_BROKER_CLUSTER_INFO), anyLong())).thenReturn(response);
        ClusterInfo got = brokerOuterAPI.getBrokerClusterInfo();

        assertEquals(want, got);
    }

    private RemotingCommand buildResponse(Boolean changed) {
        final RemotingCommand response = RemotingCommand.createResponseCommand(QueryDataVersionResponseHeader.class);
        final QueryDataVersionResponseHeader responseHeader = (QueryDataVersionResponseHeader) response.readCustomHeader();
        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        responseHeader.setChanged(changed);
        return response;
    }

    @Test
    public void testLookupAddressByDomain() throws Exception {
        init();
        brokerOuterAPI.start();
        Class<BrokerOuterAPI> clazz = BrokerOuterAPI.class;
        Method method = clazz.getDeclaredMethod("dnsLookupAddressByDomain", String.class);
        method.setAccessible(true);
        List<String> addressList = (List<String>) method.invoke(brokerOuterAPI, "localhost:6789");
        AtomicBoolean result = new AtomicBoolean(false);
        addressList.forEach(s -> {
            if (s.contains("127.0.0.1:6789")) {
                result.set(true);
            }
        });
        Assert.assertTrue(result.get());
    }

    @Test
    public void testPullMessageFromSpecificBrokerAsync_createChannel_null() throws Exception {
        NettyRemotingClient mockClient = PowerMockito.spy(new NettyRemotingClient(new NettyClientConfig()));
        PowerMockito.when(mockClient, "getAndCreateChannelAsync", any()).thenReturn(null);
        BrokerOuterAPI api = new BrokerOuterAPI(new NettyClientConfig(), new AuthConfig());
        Field field = BrokerOuterAPI.class.getDeclaredField("remotingClient");
        field.setAccessible(true);
        field.set(api, mockClient);

        Triple<PullResult, String, Boolean> rst = api.pullMessageFromSpecificBrokerAsync("", "", "", "", 1, 1, 1, 3000L).join();
        Assert.assertNull(rst.getLeft());
        Assert.assertTrue(rst.getMiddle().contains("connect"));
        Assert.assertTrue(rst.getRight()); // need retry
    }

    @Test
    public void testPullMessageFromSpecificBrokerAsync_createChannel_future_notSuccess() throws Exception {
        NettyRemotingClient mockClient = PowerMockito.spy(new NettyRemotingClient(new NettyClientConfig()));
        DefaultChannelPromise promise = PowerMockito.spy(new DefaultChannelPromise(PowerMockito.mock(Channel.class), new DefaultEventExecutor()));
        PowerMockito.when(mockClient, "getAndCreateChannelAsync", any()).thenReturn(promise);
        BrokerOuterAPI api = new BrokerOuterAPI(new NettyClientConfig(), new AuthConfig());
        Field field = BrokerOuterAPI.class.getDeclaredField("remotingClient");
        field.setAccessible(true);
        field.set(api, mockClient);

        promise.tryFailure(new Throwable());
        Triple<PullResult, String, Boolean> rst
                = api.pullMessageFromSpecificBrokerAsync("", "", "", "", 1, 1, 1, 3000L).join();
        Assert.assertNull(rst.getLeft());
        Assert.assertTrue(rst.getMiddle().contains("connect"));
        Assert.assertTrue(rst.getRight()); // need retry
    }

    // skip other future status test

    @Test
    public void testPullMessageFromSpecificBrokerAsync_timeout() throws Exception {
        Channel channel = Mockito.mock(Channel.class);
        when(channel.isActive()).thenReturn(true);
        NettyRemotingClient mockClient = PowerMockito.spy(new NettyRemotingClient(new NettyClientConfig()));
        DefaultChannelPromise promise = PowerMockito.spy(new DefaultChannelPromise(PowerMockito.mock(Channel.class), new DefaultEventExecutor()));
        PowerMockito.when(mockClient, "getAndCreateChannelAsync", any()).thenReturn(promise);
        when(promise.channel()).thenReturn(channel);
        BrokerOuterAPI api = new BrokerOuterAPI(new NettyClientConfig(), new AuthConfig());
        Field field = BrokerOuterAPI.class.getDeclaredField("remotingClient");
        field.setAccessible(true);
        field.set(api, mockClient);

        CompletableFuture<ResponseFuture> future = new CompletableFuture<>();
        doReturn(future).when(mockClient).invokeImpl(any(Channel.class), any(RemotingCommand.class), anyLong());
        promise.trySuccess(null);
        future.completeExceptionally(new RemotingTimeoutException("wait response on the channel timeout"));
        Triple<PullResult, String, Boolean> rst = api.pullMessageFromSpecificBrokerAsync("", "", "", "", 1, 1, 1, 3000L).join();
        Assert.assertNull(rst.getLeft());
        Assert.assertTrue(rst.getMiddle().contains("timeout"));
        Assert.assertTrue(rst.getRight()); // need retry
    }

    @Test
    public void testPullMessageFromSpecificBrokerAsync_brokerReturn_pullStatusCode() throws Exception {
        Channel channel = Mockito.mock(Channel.class);
        when(channel.isActive()).thenReturn(true);
        NettyRemotingClient mockClient = PowerMockito.spy(new NettyRemotingClient(new NettyClientConfig()));
        DefaultChannelPromise promise = PowerMockito.spy(new DefaultChannelPromise(PowerMockito.mock(Channel.class), new DefaultEventExecutor()));
        PowerMockito.when(mockClient, "getAndCreateChannelAsync", any()).thenReturn(promise);
        when(promise.channel()).thenReturn(channel);
        BrokerOuterAPI api = new BrokerOuterAPI(new NettyClientConfig(), new AuthConfig());
        Field field = BrokerOuterAPI.class.getDeclaredField("remotingClient");
        field.setAccessible(true);
        field.set(api, mockClient);

        int[] respCodes = new int[] {ResponseCode.SUCCESS, ResponseCode.PULL_NOT_FOUND, ResponseCode.PULL_RETRY_IMMEDIATELY, ResponseCode.PULL_OFFSET_MOVED};
        PullStatus[] respStatus = new PullStatus[] {PullStatus.FOUND, PullStatus.NO_NEW_MSG, PullStatus.NO_MATCHED_MSG, PullStatus.OFFSET_ILLEGAL};
        for (int i = 0; i < respCodes.length; i++) {
            CompletableFuture<ResponseFuture> future = new CompletableFuture<>();
            doReturn(future).when(mockClient).invokeImpl(any(Channel.class), any(RemotingCommand.class), anyLong());
            RemotingCommand response = mockPullMessageResponse(respCodes[i]);
            ResponseFuture responseFuture = new ResponseFuture(channel, 0, null, 1000,
                    resp -> {}, new SemaphoreReleaseOnlyOnce(new Semaphore(1)));
            responseFuture.setResponseCommand(response);
            promise.trySuccess(null);
            future.complete(responseFuture);

            Triple<PullResult, String, Boolean> rst = api.pullMessageFromSpecificBrokerAsync("", "", "", "", 1, 1, 1, 3000L).join();
            Assert.assertEquals(respStatus[i], rst.getLeft().getPullStatus());
            if (ResponseCode.SUCCESS == respCodes[i]) {
                Assert.assertEquals(1, rst.getLeft().getMsgFoundList().size());
            } else {
                Assert.assertNull(rst.getLeft().getMsgFoundList());
            }
            Assert.assertEquals(respStatus[i].name(), rst.getMiddle());
            Assert.assertFalse(rst.getRight()); // no retry
        }
    }

    @Test
    public void testPullMessageFromSpecificBrokerAsync_brokerReturn_allOtherResponseCode() throws Exception {
        Channel channel = Mockito.mock(Channel.class);
        when(channel.isActive()).thenReturn(true);
        NettyRemotingClient mockClient = PowerMockito.spy(new NettyRemotingClient(new NettyClientConfig()));
        DefaultChannelPromise promise = PowerMockito.spy(new DefaultChannelPromise(PowerMockito.mock(Channel.class), new DefaultEventExecutor()));
        PowerMockito.when(mockClient, "getAndCreateChannelAsync", any()).thenReturn(promise);
        when(promise.channel()).thenReturn(channel);
        BrokerOuterAPI api = new BrokerOuterAPI(new NettyClientConfig(), new AuthConfig());
        Field field = BrokerOuterAPI.class.getDeclaredField("remotingClient");
        field.setAccessible(true);
        field.set(api, mockClient);

        CompletableFuture<ResponseFuture> future = new CompletableFuture<>();
        doReturn(future).when(mockClient).invokeImpl(any(Channel.class), any(RemotingCommand.class), anyLong());
        // test one code here, skip others
        RemotingCommand response = mockPullMessageResponse(ResponseCode.SUBSCRIPTION_NOT_EXIST);
        ResponseFuture responseFuture = new ResponseFuture(channel, 0, null, 1000,
                resp -> {}, new SemaphoreReleaseOnlyOnce(new Semaphore(1)));
        responseFuture.setResponseCommand(response);
        promise.trySuccess(null);
        future.complete(responseFuture);

        Triple<PullResult, String, Boolean> rst = api.pullMessageFromSpecificBrokerAsync("", "", "", "", 1, 1, 1, 3000L).join();
        Assert.assertNull(rst.getLeft());
        Assert.assertTrue(rst.getMiddle().contains(ResponseCode.SUBSCRIPTION_NOT_EXIST + ""));
        Assert.assertTrue(rst.getRight()); // need retry
    }

    private RemotingCommand mockPullMessageResponse(int responseCode) throws Exception {
        RemotingCommand response = RemotingCommand.createResponseCommand(PullMessageResponseHeader.class);
        response.setCode(responseCode);
        if (responseCode == ResponseCode.SUCCESS) {
            MessageExt msg = new MessageExt();
            msg.setBody("HW".getBytes());
            msg.setTopic("topic");
            msg.setBornHost(new InetSocketAddress("127.0.0.1", 9000));
            msg.setStoreHost(new InetSocketAddress("127.0.0.1", 9000));
            byte[] encode = MessageDecoder.encode(msg, false);
            response.setBody(encode);
        }
        PullMessageResponseHeader responseHeader = (PullMessageResponseHeader) response.readCustomHeader();
        responseHeader.setNextBeginOffset(0L);
        responseHeader.setMaxOffset(0L);
        responseHeader.setMinOffset(0L);
        responseHeader.setOffsetDelta(0L);
        responseHeader.setTopicSysFlag(0);
        responseHeader.setGroupSysFlag(0);
        responseHeader.setSuggestWhichBrokerId(0L);
        responseHeader.setForbiddenType(0);
        response.makeCustomHeaderToNet();
        return response;
    }

}
