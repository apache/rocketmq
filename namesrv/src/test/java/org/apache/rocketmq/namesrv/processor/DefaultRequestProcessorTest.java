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

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import org.apache.rocketmq.common.namesrv.NamesrvConfig;
import org.apache.rocketmq.common.protocol.RequestCode;
import org.apache.rocketmq.common.protocol.ResponseCode;
import org.apache.rocketmq.common.protocol.header.namesrv.DeleteKVConfigRequestHeader;
import org.apache.rocketmq.common.protocol.header.namesrv.GetKVConfigRequestHeader;
import org.apache.rocketmq.common.protocol.header.namesrv.GetKVConfigResponseHeader;
import org.apache.rocketmq.common.protocol.header.namesrv.PutKVConfigRequestHeader;
import org.apache.rocketmq.namesrv.NamesrvController;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.netty.NettyServerConfig;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

public class DefaultRequestProcessorTest {
    /** Test Target */
    private DefaultRequestProcessor defaultRequestProcessor;

    private NamesrvController       namesrvController;

    private NamesrvConfig           namesrvConfig;

    private NettyServerConfig       nettyServerConfig;

    private Logger                  logger;

    @Before
    public void init() throws Exception {
        namesrvConfig = new NamesrvConfig();
        nettyServerConfig = new NettyServerConfig();

        namesrvController = new NamesrvController(namesrvConfig, nettyServerConfig);
        defaultRequestProcessor = new DefaultRequestProcessor(namesrvController);

        logger = mock(Logger.class);
        when(logger.isInfoEnabled()).thenReturn(false);
        setFinalStatic(DefaultRequestProcessor.class.getDeclaredField("log"), logger);
    }

    @Test
    public void testProcessRequest_PutKVConfig() throws RemotingCommandException {
        PutKVConfigRequestHeader header = new PutKVConfigRequestHeader();
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.PUT_KV_CONFIG,
            header);
        request.addExtField("namespace", "namespace");
        request.addExtField("key", "key");
        request.addExtField("value", "value");

        RemotingCommand response = defaultRequestProcessor.processRequest(null, request);

        assertThat(response.getCode()).isEqualTo(ResponseCode.SUCCESS);
        assertThat(response.getRemark()).isNull();

        assertThat(namesrvController.getKvConfigManager().getKVConfig("namespace", "key"))
            .isEqualTo("value");
    }

    @Test
    public void testProcessRequest_GetKVConfigReturnNotNull() throws RemotingCommandException {
        namesrvController.getKvConfigManager().putKVConfig("namespace", "key", "value");

        GetKVConfigRequestHeader header = new GetKVConfigRequestHeader();
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_KV_CONFIG,
            header);
        request.addExtField("namespace", "namespace");
        request.addExtField("key", "key");

        RemotingCommand response = defaultRequestProcessor.processRequest(null, request);

        assertThat(response.getCode()).isEqualTo(ResponseCode.SUCCESS);
        assertThat(response.getRemark()).isNull();

        GetKVConfigResponseHeader responseHeader = (GetKVConfigResponseHeader) response
            .readCustomHeader();

        assertThat(responseHeader.getValue()).isEqualTo("value");
    }

    @Test
    public void testProcessRequest_GetKVConfigReturnNull() throws RemotingCommandException {
        GetKVConfigRequestHeader header = new GetKVConfigRequestHeader();
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_KV_CONFIG,
            header);
        request.addExtField("namespace", "namespace");
        request.addExtField("key", "key");

        RemotingCommand response = defaultRequestProcessor.processRequest(null, request);

        assertThat(response.getCode()).isEqualTo(ResponseCode.QUERY_NOT_FOUND);
        assertThat(response.getRemark()).isEqualTo("No config item, Namespace: namespace Key: key");

        GetKVConfigResponseHeader responseHeader = (GetKVConfigResponseHeader) response
            .readCustomHeader();

        assertThat(responseHeader.getValue()).isNull();
    }

    @Test
    public void testProcessRequest_DeleteKVConfig() throws RemotingCommandException {
        namesrvController.getKvConfigManager().putKVConfig("namespace", "key", "value");

        DeleteKVConfigRequestHeader header = new DeleteKVConfigRequestHeader();
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.DELETE_KV_CONFIG,
            header);
        request.addExtField("namespace", "namespace");
        request.addExtField("key", "key");

        RemotingCommand response = defaultRequestProcessor.processRequest(null, request);

        assertThat(response.getCode()).isEqualTo(ResponseCode.SUCCESS);
        assertThat(response.getRemark()).isNull();

        assertThat(namesrvController.getKvConfigManager().getKVConfig("namespace", "key"))
            .isNull();
    }

    private static void setFinalStatic(Field field, Object newValue) throws Exception {
        field.setAccessible(true);
        Field modifiersField = Field.class.getDeclaredField("modifiers");
        modifiersField.setAccessible(true);
        modifiersField.setInt(field, field.getModifiers() & ~Modifier.FINAL);
        field.set(null, newValue);
    }
}