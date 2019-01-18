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

package org.apache.rocketmq.remoting;

import java.util.concurrent.Executors;
import org.apache.rocketmq.remoting.annotation.CFNullable;
import org.apache.rocketmq.remoting.common.RemotingUtil;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.exception.RemotingConnectException;
import org.apache.rocketmq.remoting.exception.RemotingSendRequestException;
import org.apache.rocketmq.remoting.exception.RemotingTimeoutException;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.serialize.LanguageCode;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertTrue;

public class RemotingHttp2InvokingTest {

    private RemotingServer remotingHttp2Server;
    private RemotingClient remotingHttp2Client;
    private int defaultRequestCode = 0;

    public RemotingServer createHttp2RemotingServer() throws InterruptedException {
        RemotingServer remotingServer = RemotingServerFactory.getInstance().createRemotingServer(RemotingUtil.HTTP2_PROTOCOL).init(new ServerConfig()
            ,null);
        remotingServer.registerProcessor(defaultRequestCode, new RequestProcessor() {
            @Override
            public RemotingCommand processRequest(RemotingChannel ctx, RemotingCommand request) {
                request.setRemark("Hi " + ctx.remoteAddress());
                return request;
            }

            @Override
            public boolean rejectRequest() {
                return false;
            }
        }, Executors.newSingleThreadExecutor());
        remotingServer.start();
        return remotingServer;
    }

    public RemotingClient createHttp2RemotingClient() {
        RemotingClient client = RemotingClientFactory.getInstance().createRemotingClient(RemotingUtil.HTTP2_PROTOCOL).init(new ClientConfig(), null);
        client.start();
        return client;
    }

    @Before
    public void setup() throws InterruptedException {
        remotingHttp2Server = createHttp2RemotingServer();
        remotingHttp2Client = createHttp2RemotingClient();
    }

    @After
    public void destroy() {
        remotingHttp2Client.shutdown();
        remotingHttp2Server.shutdown();
    }

    @Test
    public void testHttp2InvokeSync() throws InterruptedException, RemotingConnectException,
        RemotingSendRequestException, RemotingTimeoutException {
        Http2RequestHeader http2RequestHeader = new Http2RequestHeader();
        http2RequestHeader.setCount(1);
        http2RequestHeader.setMessageTitle("Welcome");
        RemotingCommand request = RemotingCommand.createRequestCommand(0, http2RequestHeader);
        RemotingCommand response = remotingHttp2Client.invokeSync("localhost:8888", request, 1000 * 5);
        assertTrue(response != null);
        assertThat(response.getLanguage()).isEqualTo(LanguageCode.JAVA);
        assertThat(response.getExtFields()).hasSize(2);

    }

    class Http2RequestHeader implements CommandCustomHeader {
        @CFNullable
        private Integer count;

        @CFNullable
        private String messageTitle;

        @Override
        public void checkFields() throws RemotingCommandException {
        }

        public Integer getCount() {
            return count;
        }

        public void setCount(Integer count) {
            this.count = count;
        }

        public String getMessageTitle() {
            return messageTitle;
        }

        public void setMessageTitle(String messageTitle) {
            this.messageTitle = messageTitle;
        }
    }
}
