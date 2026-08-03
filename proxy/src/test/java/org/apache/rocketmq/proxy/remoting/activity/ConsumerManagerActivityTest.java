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

import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.config.InitConfigTest;
import org.apache.rocketmq.proxy.processor.MessagingProcessor;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.RequestCode;
import org.apache.rocketmq.remoting.protocol.ResponseCode;
import org.apache.rocketmq.remoting.protocol.header.GetConsumerListByGroupRequestHeader;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class ConsumerManagerActivityTest extends InitConfigTest {
    private static final String CONSUMER_GROUP = "offline-group";

    private ConsumerManagerActivity consumerManagerActivity;

    @Mock
    private MessagingProcessor messagingProcessor;

    @Before
    public void setup() {
        this.consumerManagerActivity = new ConsumerManagerActivity(null, messagingProcessor);
    }

    @Test
    public void testGetConsumerListByGroupShouldReturnNotOnlineWhenGroupMissing() throws Exception {
        GetConsumerListByGroupRequestHeader header = new GetConsumerListByGroupRequestHeader();
        header.setConsumerGroup(CONSUMER_GROUP);
        RemotingCommand request = RemotingCommand.createRequestCommand(
            RequestCode.GET_CONSUMER_LIST_BY_GROUP, header);
        request.makeCustomHeaderToNet();
        when(messagingProcessor.getConsumerGroupInfo(any(), eq(CONSUMER_GROUP))).thenReturn(null);

        RemotingCommand response = consumerManagerActivity.getConsumerListByGroup(
            null, request, ProxyContext.create());

        assertThat(response.getCode()).isEqualTo(ResponseCode.CONSUMER_NOT_ONLINE);
        assertThat(response.getRemark()).isEqualTo("the consumer group[offline-group] not online");
    }
}
