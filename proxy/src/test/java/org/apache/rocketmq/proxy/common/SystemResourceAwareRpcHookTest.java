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

package org.apache.rocketmq.proxy.common;

import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.topic.TopicValidator;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.RequestCode;
import org.apache.rocketmq.remoting.protocol.header.SendMessageRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.SendMessageRequestHeaderV2;
import org.apache.rocketmq.remoting.protocol.header.UnregisterClientRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.namesrv.GetRouteInfoRequestHeader;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;

public class SystemResourceAwareRpcHookTest {
    private RecordingHook userHook;
    private RecordingHook systemHook;

    private SystemResourceAwareRpcHook rpcHook;
    private final String remoteAddr = "127.0.0.1:9876";

    @Before
    public void setUp() {
        userHook = new RecordingHook();
        systemHook = new RecordingHook();
        rpcHook = new SystemResourceAwareRpcHook(userHook, systemHook);
        InternalContextHolder.clear();
    }

    @After
    public void tearDown() {
        InternalContextHolder.clear();
    }

    @Test
    public void testExternalClientAttackPrevented() {
        SendMessageRequestHeader header = new SendMessageRequestHeader();
        header.setProducerGroup(MixAll.CLIENT_INNER_PRODUCER_GROUP);
        header.setTopic("user_topic");
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.SEND_MESSAGE, header);

        request.makeCustomHeaderToNet();

        rpcHook.doBeforeRequest(remoteAddr, request);

        assertEquals(1, userHook.beforeRequestCount);
        assertSame(request, userHook.lastBeforeRequest);
        assertEquals(remoteAddr, userHook.lastBeforeRemoteAddr);
        assertEquals(0, systemHook.beforeRequestCount);
    }

    @Test
    public void testSystemHookUsedForInternalSystemRequest() {
        InternalContextHolder.beginInternalScope();

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.SEND_MESSAGE, null);
        request.addExtField("producerGroup", MixAll.CLIENT_INNER_PRODUCER_GROUP);
        request.addExtField("topic", TopicValidator.AUTO_CREATE_TOPIC_KEY_TOPIC);

        rpcHook.doBeforeRequest(remoteAddr, request);

        assertEquals(1, systemHook.beforeRequestCount);
        assertSame(request, systemHook.lastBeforeRequest);
        assertEquals(remoteAddr, systemHook.lastBeforeRemoteAddr);
        assertEquals(0, userHook.beforeRequestCount);
    }

    @Test
    public void testInternalScopeWithNonSystemResourceRoute() {
        InternalContextHolder.beginInternalScope();

        SendMessageRequestHeader header = new SendMessageRequestHeader();
        header.setProducerGroup("STANDARD_USER_GROUP");
        header.setTopic("STANDARD_USER_TOPIC");
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.SEND_MESSAGE, header);

        request.makeCustomHeaderToNet();

        rpcHook.doBeforeRequest(remoteAddr, request);

        assertEquals(1, userHook.beforeRequestCount);
        assertSame(request, userHook.lastBeforeRequest);
        assertEquals(remoteAddr, userHook.lastBeforeRemoteAddr);
        assertEquals(0, systemHook.beforeRequestCount);
    }

    @Test
    public void testGetRouteInfoSystemTopicRoute() {
        InternalContextHolder.beginInternalScope();

        GetRouteInfoRequestHeader header = new GetRouteInfoRequestHeader();
        header.setTopic(TopicValidator.AUTO_CREATE_TOPIC_KEY_TOPIC);
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_ROUTEINFO_BY_TOPIC, header);

        request.makeCustomHeaderToNet();

        rpcHook.doBeforeRequest(remoteAddr, request);

        assertEquals(1, systemHook.beforeRequestCount);
        assertSame(request, systemHook.lastBeforeRequest);
        assertEquals(remoteAddr, systemHook.lastBeforeRemoteAddr);
        assertEquals(0, userHook.beforeRequestCount);
    }

    @Test
    public void testUnregisterClientStrictVerification() {
        InternalContextHolder.beginInternalScope();

        UnregisterClientRequestHeader headerA = new UnregisterClientRequestHeader();
        headerA.setClientID("clientA");
        headerA.setConsumerGroup(MixAll.TOOLS_CONSUMER_GROUP);

        RemotingCommand reqA = RemotingCommand.createRequestCommand(RequestCode.UNREGISTER_CLIENT, headerA);
        reqA.makeCustomHeaderToNet();

        rpcHook.doBeforeRequest(remoteAddr, reqA);

        assertEquals(1, systemHook.beforeRequestCount);
        assertSame(reqA, systemHook.lastBeforeRequest);
        assertEquals(remoteAddr, systemHook.lastBeforeRemoteAddr);

        InternalContextHolder.clear();
        InternalContextHolder.beginInternalScope();

        UnregisterClientRequestHeader headerB = new UnregisterClientRequestHeader();
        headerB.setClientID("clientB");
        headerB.setConsumerGroup("NORMAL_CONSUMER_GROUP");

        RemotingCommand reqB = RemotingCommand.createRequestCommand(RequestCode.UNREGISTER_CLIENT, headerB);
        reqB.makeCustomHeaderToNet();

        rpcHook.doBeforeRequest(remoteAddr, reqB);

        assertEquals(1, userHook.beforeRequestCount);
        assertSame(reqB, userHook.lastBeforeRequest);
        assertEquals(remoteAddr, userHook.lastBeforeRemoteAddr);
    }

    @Test
    public void testDoAfterResponseRouting() {
        InternalContextHolder.beginInternalScope();

        SendMessageRequestHeader header = new SendMessageRequestHeader();
        header.setProducerGroup(MixAll.CLIENT_INNER_PRODUCER_GROUP);
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.SEND_MESSAGE, header);

        request.makeCustomHeaderToNet();

        RemotingCommand response = RemotingCommand.createResponseCommand(0, "SUCCESS");

        rpcHook.doAfterResponse(remoteAddr, request, response);

        assertEquals(1, systemHook.afterResponseCount);
        assertSame(request, systemHook.lastAfterRequest);
        assertSame(response, systemHook.lastAfterResponse);
        assertEquals(remoteAddr, systemHook.lastAfterRemoteAddr);
        assertEquals(0, userHook.afterResponseCount);

        InternalContextHolder.clear();
        rpcHook.doAfterResponse(remoteAddr, request, response);

        assertEquals(1, userHook.afterResponseCount);
        assertSame(request, userHook.lastAfterRequest);
        assertSame(response, userHook.lastAfterResponse);
        assertEquals(remoteAddr, userHook.lastAfterRemoteAddr);
    }

    @Test
    public void testFallbackExtFieldsValidSystemMatch() {
        InternalContextHolder.beginInternalScope();

        RemotingCommand request = RemotingCommand.createRequestCommand(9999, null);
        request.addExtField("topic", TopicValidator.AUTO_CREATE_TOPIC_KEY_TOPIC);

        rpcHook.doBeforeRequest(remoteAddr, request);

        assertEquals(1, systemHook.beforeRequestCount);
        assertSame(request, systemHook.lastBeforeRequest);
        assertEquals(remoteAddr, systemHook.lastBeforeRemoteAddr);
        assertEquals(0, userHook.beforeRequestCount);
    }

    @Test
    public void testFallbackExtFieldsRejection() {
        InternalContextHolder.beginInternalScope();

        RemotingCommand request = RemotingCommand.createRequestCommand(9999, null);
        request.addExtField("topic", "standard_user_topic");
        request.addExtField("producerGroup", "standard_user_group");

        rpcHook.doBeforeRequest(remoteAddr, request);

        assertEquals(1, userHook.beforeRequestCount);
        assertSame(request, userHook.lastBeforeRequest);
        assertEquals(remoteAddr, userHook.lastBeforeRemoteAddr);
        assertEquals(0, systemHook.beforeRequestCount);
    }

    @Test
    public void testFallbackExtFieldsNullMap() {
        InternalContextHolder.beginInternalScope();

        RemotingCommand request = RemotingCommand.createRequestCommand(9999, null);

        rpcHook.doBeforeRequest(remoteAddr, request);

        assertEquals(1, userHook.beforeRequestCount);
        assertSame(request, userHook.lastBeforeRequest);
        assertEquals(remoteAddr, userHook.lastBeforeRemoteAddr);
        assertEquals(0, systemHook.beforeRequestCount);
        assertNull(systemHook.lastBeforeRequest);
    }

    @Test
    public void testNullRequestFallsBackToUserHook() {
        InternalContextHolder.beginInternalScope();

        rpcHook.doBeforeRequest(remoteAddr, null);

        assertEquals(1, userHook.beforeRequestCount);
        assertNull(userHook.lastBeforeRequest);
        assertEquals(remoteAddr, userHook.lastBeforeRemoteAddr);

        assertEquals(0, systemHook.beforeRequestCount);
    }

    @Test
    public void testSendMessageV2SystemRouting() {
        InternalContextHolder.beginInternalScope();

        SendMessageRequestHeaderV2 headerV2 = new SendMessageRequestHeaderV2();

        headerV2.setA(MixAll.CLIENT_INNER_PRODUCER_GROUP);
        headerV2.setB(TopicValidator.AUTO_CREATE_TOPIC_KEY_TOPIC);

        RemotingCommand request = RemotingCommand.createRequestCommand(
            RequestCode.SEND_MESSAGE_V2,
            headerV2
        );

        request.makeCustomHeaderToNet();

        rpcHook.doBeforeRequest(remoteAddr, request);

        assertEquals(1, systemHook.beforeRequestCount);
        assertSame(request, systemHook.lastBeforeRequest);
        assertEquals(remoteAddr, systemHook.lastBeforeRemoteAddr);

        assertEquals(0, userHook.beforeRequestCount);
    }

    @Test
    public void testExceptionDuringHeaderDecodeFallsBackToUserHook() {
        InternalContextHolder.beginInternalScope();

        RemotingCommand request = RemotingCommand.createRequestCommand(
            RequestCode.SEND_MESSAGE,
            null
        );

        request.addExtField("producerGroup", "groupOnly");

        rpcHook.doBeforeRequest(remoteAddr, request);

        assertEquals(1, userHook.beforeRequestCount);
        assertSame(request, userHook.lastBeforeRequest);
        assertEquals(remoteAddr, userHook.lastBeforeRemoteAddr);

        assertEquals(0, systemHook.beforeRequestCount);
    }

    @Test
    public void testFallbackExtFieldsProducerGroupSystemMatch() {
        InternalContextHolder.beginInternalScope();

        RemotingCommand request = RemotingCommand.createRequestCommand(9999, null);

        request.addExtField(
            "producerGroup",
            MixAll.CLIENT_INNER_PRODUCER_GROUP
        );

        rpcHook.doBeforeRequest(remoteAddr, request);

        assertEquals(1, systemHook.beforeRequestCount);
        assertSame(request, systemHook.lastBeforeRequest);
        assertEquals(remoteAddr, systemHook.lastBeforeRemoteAddr);

        assertEquals(0, userHook.beforeRequestCount);
    }

    @Test
    public void testFallbackExtFieldsConsumerGroupSystemMatch() {
        InternalContextHolder.beginInternalScope();

        RemotingCommand request = RemotingCommand.createRequestCommand(9999, null);

        request.addExtField(
            "consumerGroup",
            MixAll.TOOLS_CONSUMER_GROUP
        );

        rpcHook.doBeforeRequest(remoteAddr, request);

        assertEquals(1, systemHook.beforeRequestCount);
        assertSame(request, systemHook.lastBeforeRequest);
        assertEquals(remoteAddr, systemHook.lastBeforeRemoteAddr);

        assertEquals(0, userHook.beforeRequestCount);
    }

    private static class RecordingHook implements RPCHook {
        private int beforeRequestCount;
        private int afterResponseCount;
        private String lastBeforeRemoteAddr;
        private String lastAfterRemoteAddr;
        private RemotingCommand lastBeforeRequest;
        private RemotingCommand lastAfterRequest;
        private RemotingCommand lastAfterResponse;

        @Override
        public void doBeforeRequest(String remoteAddr, RemotingCommand request) {
            beforeRequestCount++;
            lastBeforeRemoteAddr = remoteAddr;
            lastBeforeRequest = request;
        }

        @Override
        public void doAfterResponse(String remoteAddr, RemotingCommand request, RemotingCommand response) {
            afterResponseCount++;
            lastAfterRemoteAddr = remoteAddr;
            lastAfterRequest = request;
            lastAfterResponse = response;
        }
    }
}
