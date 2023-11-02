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

package org.apache.rocketmq.controller;

import java.nio.charset.StandardCharsets;
import java.util.Properties;
import org.apache.rocketmq.common.ControllerConfig;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.controller.processor.ControllerRequestProcessor;
import org.apache.rocketmq.remoting.netty.NettyClientConfig;
import org.apache.rocketmq.remoting.netty.NettyServerConfig;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.RequestCode;
import org.apache.rocketmq.remoting.protocol.ResponseCode;
import org.junit.Before;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class ControllerRequestProcessorTest {

    private ControllerRequestProcessor controllerRequestProcessor;

    @Before
    public void init() throws Exception {
        controllerRequestProcessor = new ControllerRequestProcessor(new ControllerManager(new ControllerConfig(), new NettyServerConfig(), new NettyClientConfig()));
    }

    @Test
    public void testProcessRequest_UpdateConfigPath() throws Exception {
        final RemotingCommand updateConfigRequest = RemotingCommand.createRequestCommand(RequestCode.UPDATE_CONTROLLER_CONFIG, null);
        Properties properties = new Properties();

        // Update allowed value
        properties.setProperty("notifyBrokerRoleChanged", "true");
        updateConfigRequest.setBody(MixAll.properties2String(properties).getBytes(StandardCharsets.UTF_8));

        RemotingCommand response = controllerRequestProcessor.processRequest(null, updateConfigRequest);

        assertThat(response).isNotNull();
        assertThat(response.getCode()).isEqualTo(ResponseCode.SUCCESS);

        // Update disallowed value
        properties.clear();
        properties.setProperty("configStorePath", "test/path");
        updateConfigRequest.setBody(MixAll.properties2String(properties).getBytes(StandardCharsets.UTF_8));

        response = controllerRequestProcessor.processRequest(null, updateConfigRequest);

        assertThat(response).isNotNull();
        assertThat(response.getCode()).isEqualTo(ResponseCode.NO_PERMISSION);
        assertThat(response.getRemark()).contains("Can not update config path");

    }
}
