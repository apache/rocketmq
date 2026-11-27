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
package org.apache.rocketmq.container;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;
import org.apache.rocketmq.remoting.netty.NettyClientConfig;
import org.apache.rocketmq.remoting.netty.NettyServerConfig;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.RequestCode;
import org.apache.rocketmq.remoting.protocol.ResponseCode;
import org.apache.rocketmq.remoting.protocol.header.AddBrokerRequestHeader;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class BrokerContainerProcessorTest {

    @Test
    public void testListenPortValidationRejectsEphemeralAndNegativePorts() {
        assertFalse(BrokerContainer.isValidListenPort(-1));
        assertFalse(BrokerContainer.isValidListenPort(0));
        assertTrue(BrokerContainer.isValidListenPort(10911));
        assertEquals("listenPort must be greater than 0 for brokers in a container",
            BrokerContainer.INVALID_LISTEN_PORT_MESSAGE);
    }

    @Test
    public void testAddBrokerRejectsEphemeralPortBeforeRegistration() throws Exception {
        Path configPath = Files.createTempFile("rocketmq-broker", ".properties");
        Properties brokerProperties = new Properties();
        brokerProperties.setProperty("listenPort", "0");
        try (OutputStream outputStream = Files.newOutputStream(configPath)) {
            brokerProperties.store(outputStream, null);
        }

        BrokerContainer brokerContainer = new BrokerContainer(
            new BrokerContainerConfig(),
            new NettyServerConfig(),
            new NettyClientConfig());
        BrokerContainerProcessor processor = new BrokerContainerProcessor(brokerContainer);
        AddBrokerRequestHeader requestHeader = new AddBrokerRequestHeader();
        requestHeader.setConfigPath(configPath.toString());
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.ADD_BROKER, requestHeader);
        request.makeCustomHeaderToNet();
        ChannelHandlerContext context = mock(ChannelHandlerContext.class);
        Channel channel = mock(Channel.class);
        when(channel.remoteAddress()).thenReturn(new InetSocketAddress("127.0.0.1", 12345));
        when(context.channel()).thenReturn(channel);

        try {
            RemotingCommand response = processor.processRequest(context, request);

            assertEquals(ResponseCode.SYSTEM_ERROR, response.getCode());
            assertEquals(BrokerContainer.INVALID_LISTEN_PORT_MESSAGE, response.getRemark());
            assertTrue(brokerContainer.getMasterBrokers().isEmpty());
        } finally {
            brokerContainer.shutdown();
            Files.deleteIfExists(configPath);
        }
    }
}
