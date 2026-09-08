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

import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.concurrent.CompletableFuture;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.rocketmq.broker.out.BrokerOuterAPI;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.remoting.RemotingClient;
import org.apache.rocketmq.remoting.exception.RemotingConnectException;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.ResponseCode;
import org.apache.rocketmq.remoting.protocol.body.BrokerMemberGroup;
import org.apache.rocketmq.remoting.protocol.body.GetBrokerMemberGroupResponseBody;
import org.apache.rocketmq.remoting.protocol.route.BrokerData;
import org.apache.rocketmq.remoting.protocol.route.TopicRouteData;
import org.apache.rocketmq.store.MessageStore;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class BrokerMemberLookupTest {
    private final RemotingClient client = mock(RemotingClient.class);
    private final BrokerOuterAPI outerAPI = mock(BrokerOuterAPI.class, CALLS_REAL_METHODS);
    private final BrokerController controller = mock(BrokerController.class);
    private final BrokerConfig config = new BrokerConfig();
    private final BrokerPreOnlineService service = spy(new BrokerPreOnlineService(controller));

    private void setup(boolean compatible) throws Exception {
        config.setBrokerClusterName("test-cluster");
        config.setBrokerName("test-broker");
        config.setBrokerId(0L);
        config.setCompatibleWithOldNameSrv(compatible);
        config.setEnableSlaveActingMaster(true);
        config.setSkipPreOnline(false);
        FieldUtils.writeField(outerAPI, "remotingClient", client, true);
        when(controller.getBrokerConfig()).thenReturn(config);
        when(controller.getBrokerOuterAPI()).thenReturn(outerAPI);
        when(controller.getBrokerAddr()).thenReturn("127.0.0.1:10911");
    }

    private boolean prepare() throws Exception {
        Method method = BrokerPreOnlineService.class.getDeclaredMethod("prepareForBrokerOnline");
        method.setAccessible(true);
        return (boolean) method.invoke(service);
    }

    private void respond(RemotingCommand response) throws Exception {
        when(client.invokeSync(isNull(), any(RemotingCommand.class), anyLong())).thenReturn(response);
    }

    private void assertStartsWithoutHandshake() throws Exception {
        assertTrue(prepare());
        verify(controller).startService(0L, "127.0.0.1:10911");
        verify(service, never()).waitForHaHandshakeComplete(anyString());
    }

    @Test
    public void compatibleSystemErrorKeepsBrokerOffline() throws Exception {
        setup(true);
        respond(RemotingCommand.createResponseCommand(ResponseCode.SYSTEM_ERROR, "name server not ready"));
        assertFalse(prepare());
        verify(controller, never()).startService(anyLong(), anyString());
    }

    @Test
    public void nativeSystemErrorKeepsBrokerOffline() throws Exception {
        setup(false);
        respond(RemotingCommand.createResponseCommand(ResponseCode.SYSTEM_ERROR, "name server not ready"));
        assertFalse(prepare());
        verify(controller, never()).startService(anyLong(), anyString());
    }

    @Test
    public void compatibleConnectionFailureKeepsBrokerOffline() throws Exception {
        assertConnectionFailure(false);
    }

    @Test
    public void nativeConnectionFailureKeepsBrokerOffline() throws Exception {
        assertConnectionFailure(true);
    }

    private void assertConnectionFailure(boolean nativeQuery) throws Exception {
        setup(!nativeQuery);
        when(client.invokeSync(isNull(), any(RemotingCommand.class), anyLong()))
            .thenThrow(new RemotingConnectException("namesrv"));
        assertFalse(prepare());
        verify(controller, never()).startService(anyLong(), anyString());
    }

    @Test
    public void knownPeerWithFailedHandshakeKeepsBrokerOffline() throws Exception {
        setup(true);
        HashMap<Long, String> addresses = new HashMap<>();
        addresses.put(1L, "127.0.0.1:20911");
        TopicRouteData route = new TopicRouteData();
        route.setBrokerDatas(Collections.singletonList(new BrokerData("test-cluster", "test-broker", addresses)));
        RemotingCommand response = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, null);
        response.setBody(route.encode());
        respond(response);
        when(controller.getMessageStore()).thenReturn(mock(MessageStore.class));
        doNothing().when(outerAPI).sendBrokerHaInfo(anyString(), any(), anyLong(), anyString());
        doReturn(CompletableFuture.completedFuture(false)).when(service).waitForHaHandshakeComplete(anyString());
        assertFalse(prepare());
        verify(service).waitForHaHandshakeComplete("127.0.0.1:20911");
        verify(controller, never()).startService(anyLong(), anyString());
    }

    @Test
    public void compatibleTopicNotExistAllowsFirstBrokerOnline() throws Exception {
        setup(true);
        respond(RemotingCommand.createResponseCommand(ResponseCode.TOPIC_NOT_EXIST, "no members"));
        assertStartsWithoutHandshake();
    }

    @Test
    public void nativeSuccessfulEmptyGroupAllowsFirstBrokerOnline() throws Exception {
        setup(false);
        GetBrokerMemberGroupResponseBody body = new GetBrokerMemberGroupResponseBody();
        body.setBrokerMemberGroup(new BrokerMemberGroup("test-cluster", "test-broker"));
        RemotingCommand response = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, null);
        response.setBody(body.encode());
        respond(response);
        assertStartsWithoutHandshake();
    }

    @Test
    public void errorResponsePreservesCodeAndRemark() throws Exception {
        for (boolean compatible : new boolean[] {true, false}) {
            setup(compatible);
            respond(RemotingCommand.createResponseCommand(ResponseCode.SYSTEM_ERROR, "name server not ready"));
            try {
                outerAPI.syncBrokerMemberGroup("test-cluster", "test-broker", compatible);
                fail("A failed member query must throw instead of returning an empty group");
            } catch (MQBrokerException e) {
                assertEquals(ResponseCode.SYSTEM_ERROR, e.getResponseCode());
                assertEquals("name server not ready", e.getErrorMessage());
            }
        }
    }

    @Test
    public void missingResponseBodyKeepsBrokerOffline() throws Exception {
        for (boolean compatible : new boolean[] {true, false}) {
            setup(compatible);
            respond(RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, null));
            assertFalse(prepare());
        }
        verify(controller, never()).startService(anyLong(), anyString());
    }

    @Test
    public void missingMemberGroupKeepsBrokerOffline() throws Exception {
        setup(false);
        RemotingCommand response = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, null);
        response.setBody(new GetBrokerMemberGroupResponseBody().encode());
        respond(response);
        assertFalse(prepare());
        verify(controller, never()).startService(anyLong(), anyString());
    }

    @Test
    public void missingBrokerRouteDataKeepsBrokerOffline() throws Exception {
        setup(true);
        for (String body : new String[] {"null", "{\"brokerDatas\":null}"}) {
            RemotingCommand response = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, null);
            response.setBody(body.getBytes(StandardCharsets.UTF_8));
            respond(response);
            assertFalse(prepare());
        }
        verify(controller, never()).startService(anyLong(), anyString());
    }

    @Test
    public void nativeTopicNotExistIsNotASuccessfulEmptyLookup() throws Exception {
        setup(false);
        respond(RemotingCommand.createResponseCommand(ResponseCode.TOPIC_NOT_EXIST, "unexpected response"));
        assertFalse(prepare());
        verify(controller, never()).startService(anyLong(), anyString());
    }

    @Test
    public void retryCanStartBrokerAfterNameServerBecomesReady() throws Exception {
        setup(true);
        when(client.invokeSync(isNull(), any(RemotingCommand.class), anyLong()))
            .thenReturn(RemotingCommand.createResponseCommand(ResponseCode.SYSTEM_ERROR, "name server not ready"))
            .thenReturn(RemotingCommand.createResponseCommand(ResponseCode.TOPIC_NOT_EXIST, "no members"));
        assertFalse(prepare());
        verify(controller, never()).startService(anyLong(), anyString());
        assertStartsWithoutHandshake();
    }
}
