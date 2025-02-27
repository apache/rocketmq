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

package org.apache.rocketmq.client.rpchook;

import org.apache.rocketmq.client.ClientConfig;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.RequestCode;
import org.apache.rocketmq.remoting.protocol.header.PullMessageRequestHeader;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class NamespaceRpcHookTest {
    private NamespaceRpcHook namespaceRpcHook;
    private ClientConfig clientConfig;
    private String namespace = "namespace";


    @Test
    public void testDoBeforeRequestWithNamespace() {
        clientConfig = new ClientConfig();
        clientConfig.setNamespaceV2(namespace);
        namespaceRpcHook = new NamespaceRpcHook(clientConfig);
        PullMessageRequestHeader pullMessageRequestHeader = new PullMessageRequestHeader();
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.PULL_MESSAGE, pullMessageRequestHeader);
        namespaceRpcHook.doBeforeRequest("", request);
        assertThat(request.getExtFields().get(MixAll.RPC_REQUEST_HEADER_NAMESPACED_FIELD)).isEqualTo("true");
        assertThat(request.getExtFields().get(MixAll.RPC_REQUEST_HEADER_NAMESPACE_FIELD)).isEqualTo(namespace);
    }

    @Test
    public void testDoBeforeRequestWithoutNamespace() {
        clientConfig = new ClientConfig();
        namespaceRpcHook = new NamespaceRpcHook(clientConfig);
        PullMessageRequestHeader pullMessageRequestHeader = new PullMessageRequestHeader();
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.PULL_MESSAGE, pullMessageRequestHeader);
        namespaceRpcHook.doBeforeRequest("", request);
        assertThat(request.getExtFields()).isNull();
    }
}