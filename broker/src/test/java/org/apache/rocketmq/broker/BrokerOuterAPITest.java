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
import io.netty.channel.ChannelHandlerContext;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.rocketmq.auth.config.AuthConfig;
import org.apache.rocketmq.broker.out.BrokerOuterAPI;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.BrokerIdentity;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.remoting.exception.RemotingTimeoutException;
import org.apache.rocketmq.remoting.netty.NettyClientConfig;
import org.apache.rocketmq.remoting.netty.NettyRemotingClient;
import org.apache.rocketmq.remoting.netty.NettyServerConfig;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;
import org.apache.rocketmq.remoting.protocol.RequestCode;
import org.apache.rocketmq.remoting.protocol.ResponseCode;
import org.apache.rocketmq.remoting.protocol.body.ClusterInfo;
import org.apache.rocketmq.remoting.protocol.body.TopicConfigSerializeWrapper;
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
import org.mockito.invocation.InvocationOnMock;
import org.mockito.junit.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.AdditionalMatchers.or;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
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
}
