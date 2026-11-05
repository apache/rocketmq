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
package org.apache.rocketmq.proxy.remoting.activity;

import org.apache.rocketmq.common.filter.ExpressionType;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.config.ConfigurationManager;
import org.apache.rocketmq.proxy.config.InitConfigTest;
import org.apache.rocketmq.proxy.processor.MessagingProcessor;
import org.apache.rocketmq.proxy.remoting.channel.RemotingChannelManager;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.RequestCode;
import org.apache.rocketmq.remoting.protocol.ResponseCode;
import org.apache.rocketmq.remoting.protocol.body.CheckClientRequestBody;
import org.apache.rocketmq.remoting.protocol.heartbeat.SubscriptionData;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static org.assertj.core.api.Assertions.assertThat;

@RunWith(MockitoJUnitRunner.class)
public class ClientManagerActivityTest extends InitConfigTest {

    private ClientManagerActivity clientManagerActivity;
    private boolean originalEnablePropertyFilter;
    @Mock
    private MessagingProcessor messagingProcessor;
    @Mock
    private RemotingChannelManager remotingChannelManager;

    @Before
    public void setUp() {
        this.clientManagerActivity = new ClientManagerActivity(null, messagingProcessor, remotingChannelManager);
        this.originalEnablePropertyFilter = ConfigurationManager.getProxyConfig().isEnablePropertyFilter();
    }

    @After
    public void tearDown() {
        ConfigurationManager.getProxyConfig().setEnablePropertyFilter(originalEnablePropertyFilter);
    }

    @Test
    public void testCheckClientConfigWithTagExpression() {
        RemotingCommand response = clientManagerActivity.checkClientConfig(null,
            createRequest(ExpressionType.TAG, "tagA || tagB"), ProxyContext.create());

        assertThat(response.getCode()).isEqualTo(ResponseCode.SUCCESS);
    }

    @Test
    public void testCheckClientConfigRejectsPropertyFilterWhenDisabled() {
        RemotingCommand response = clientManagerActivity.checkClientConfig(null,
            createRequest(ExpressionType.SQL92, "a is not null"), ProxyContext.create());

        assertThat(response.getCode()).isEqualTo(ResponseCode.SYSTEM_ERROR);
        assertThat(response.getRemark()).contains("enablePropertyFilter", ExpressionType.SQL92);
    }

    @Test
    public void testCheckClientConfigRejectsInvalidPropertyFilterExpression() {
        ConfigurationManager.getProxyConfig().setEnablePropertyFilter(true);

        RemotingCommand response = clientManagerActivity.checkClientConfig(null,
            createRequest(ExpressionType.SQL92, "a = "), ProxyContext.create());

        assertThat(response.getCode()).isEqualTo(ResponseCode.SUBSCRIPTION_PARSE_FAILED);
    }

    @Test
    public void testCheckClientConfigAcceptsValidPropertyFilterExpression() {
        ConfigurationManager.getProxyConfig().setEnablePropertyFilter(true);

        RemotingCommand response = clientManagerActivity.checkClientConfig(null,
            createRequest(ExpressionType.SQL92, "a is not null"), ProxyContext.create());

        assertThat(response.getCode()).isEqualTo(ResponseCode.SUCCESS);
    }

    @Test
    public void testCheckClientConfigRejectsRequestWithoutBody() {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.CHECK_CLIENT_CONFIG, null);

        RemotingCommand response = clientManagerActivity.checkClientConfig(null, request, ProxyContext.create());

        assertThat(response.getCode()).isEqualTo(ResponseCode.SUBSCRIPTION_PARSE_FAILED);
        assertThat(response.getRemark()).contains("body");
    }

    private RemotingCommand createRequest(String expressionType, String expression) {
        SubscriptionData subscriptionData = new SubscriptionData();
        subscriptionData.setTopic("topic");
        subscriptionData.setExpressionType(expressionType);
        subscriptionData.setSubString(expression);

        CheckClientRequestBody requestBody = new CheckClientRequestBody();
        requestBody.setClientId("clientId");
        requestBody.setGroup("group");
        requestBody.setSubscriptionData(subscriptionData);

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.CHECK_CLIENT_CONFIG, null);
        request.setBody(requestBody.encode());
        return request;
    }
}
