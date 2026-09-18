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

package org.apache.rocketmq.namesrv.processor;

import org.apache.rocketmq.common.namesrv.NamesrvConfig;
import org.apache.rocketmq.remoting.netty.NettyServerConfig;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.RequestCode;
import org.apache.rocketmq.remoting.protocol.ResponseCode;
import org.apache.rocketmq.namesrv.NamesrvController;
import org.junit.Before;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class DefaultRequestProcessorPutKVConfigTest {

    private DefaultRequestProcessor defaultRequestProcessor;

    private NamesrvController namesrvController;

    @Before
    public void init() {
        namesrvController = new NamesrvController(new NamesrvConfig(), new NettyServerConfig());
        defaultRequestProcessor = new DefaultRequestProcessor(namesrvController);
    }

    @Test
    public void testPutKVConfig() throws Exception {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.PUT_KV_CONFIG, null);
        request.addExtField("namespace", "namespace");
        request.addExtField("key", "key");
        request.addExtField("value", "value");

        RemotingCommand response = defaultRequestProcessor.processRequest(null, request);

        assertThat(response.getCode()).isEqualTo(ResponseCode.SUCCESS);
        assertThat(namesrvController.getKvConfigManager().getKVConfig("namespace", "key"))
            .isEqualTo("value");
    }

    @Test
    public void testPutKVConfigWithoutValue() throws Exception {
        // A request without the value field decodes to a null value (the
        // @CFNotNull violation of the reflective decode is logged and
        // swallowed), so the processor must reject it itself instead of
        // storing an entry that every later read reports as not found.
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.PUT_KV_CONFIG, null);
        request.addExtField("namespace", "namespace");
        request.addExtField("key", "key");

        RemotingCommand response = defaultRequestProcessor.processRequest(null, request);

        assertThat(response.getCode()).isEqualTo(ResponseCode.SYSTEM_ERROR);
        assertThat(response.getRemark()).contains("value");
        assertThat(namesrvController.getKvConfigManager().getKVConfig("namespace", "key")).isNull();
    }
}
