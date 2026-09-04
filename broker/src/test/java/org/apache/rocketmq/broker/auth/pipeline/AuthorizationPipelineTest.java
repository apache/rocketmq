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
package org.apache.rocketmq.broker.auth.pipeline;

import io.netty.channel.ChannelHandlerContext;
import java.util.Collections;
import java.util.List;
import org.apache.rocketmq.auth.authorization.context.AuthorizationContext;
import org.apache.rocketmq.auth.config.AuthConfig;
import org.apache.rocketmq.common.AbortProcessException;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.RequestCode;
import org.apache.rocketmq.remoting.protocol.ResponseCode;
import org.apache.rocketmq.remoting.protocol.header.ViewMessageRequestHeader;
import org.apache.rocketmq.remoting.protocol.heartbeat.HeartbeatData;
import org.apache.rocketmq.remoting.protocol.heartbeat.ProducerData;
import org.junit.Assert;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThatCode;

public class AuthorizationPipelineTest {

    @Test
    public void allowsCompatibleRequestWithEmptyContexts() throws Exception {
        AuthorizationPipeline pipeline = createPipeline();
        ProducerData producerData = new ProducerData();
        producerData.setGroupName("producerGroup");
        HeartbeatData heartbeatData = new HeartbeatData();
        heartbeatData.getProducerDataSet().add(producerData);
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.HEART_BEAT, null);
        request.setBody(heartbeatData.encode());

        assertThatCode(() -> pipeline.execute(null, request)).doesNotThrowAnyException();
    }

    @Test
    public void rejectsUnsupportedRequestWithEmptyContexts() {
        AuthorizationPipeline pipeline = createPipeline();
        RemotingCommand request = RemotingCommand.createRequestCommand(-1, null);

        AbortProcessException exception = Assert.assertThrows(AbortProcessException.class,
            () -> pipeline.execute(null, request));
        Assert.assertEquals(ResponseCode.NO_PERMISSION, exception.getResponseCode());
    }

    @Test
    public void rejectsTopicLessViewMessageWithEmptyContexts() {
        AuthorizationPipeline pipeline = createPipeline();
        ViewMessageRequestHeader header = new ViewMessageRequestHeader();
        header.setOffset(0L);
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.VIEW_MESSAGE_BY_ID, header);
        request.makeCustomHeaderToNet();

        AbortProcessException exception = Assert.assertThrows(AbortProcessException.class,
            () -> pipeline.execute(null, request));
        Assert.assertEquals(ResponseCode.NO_PERMISSION, exception.getResponseCode());
    }

    private AuthorizationPipeline createPipeline() {
        AuthConfig authConfig = new AuthConfig();
        authConfig.setConfigName("broker-authorization-pipeline-test");
        authConfig.setAuthorizationEnabled(true);
        return new AuthorizationPipeline(authConfig) {
            @Override
            protected List<AuthorizationContext> newContexts(ChannelHandlerContext ctx,
                RemotingCommand request) {
                return Collections.emptyList();
            }
        };
    }
}
