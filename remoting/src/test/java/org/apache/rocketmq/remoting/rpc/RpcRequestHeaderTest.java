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

package org.apache.rocketmq.remoting.rpc;

import java.nio.ByteBuffer;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.RequestCode;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class RpcRequestHeaderTest {
    String brokerName = "brokerName1";
    String namespace = "namespace1";
    boolean namespaced = true;
    boolean oneway = false;
    static class TestRequestHeader extends RpcRequestHeader {

        @Override
        public void checkFields() throws RemotingCommandException {

        }
    }

    @Test
    public void testEncodeDecode() throws RemotingCommandException {
        TestRequestHeader requestHeader = new TestRequestHeader();
        requestHeader.setBrokerName(brokerName);
        requestHeader.setNamespace(namespace);
        requestHeader.setNamespaced(namespaced);
        requestHeader.setOneway(oneway);

        RemotingCommand remotingCommand = RemotingCommand.createRequestCommand(RequestCode.PULL_MESSAGE, requestHeader);
        ByteBuffer buffer = remotingCommand.encode();

        //Simulate buffer being read in NettyDecoder
        buffer.getInt();
        byte[] bytes = new byte[buffer.limit() - 4];
        buffer.get(bytes, 0, buffer.limit() - 4);
        buffer = ByteBuffer.wrap(bytes);

        RemotingCommand decodeRequest = RemotingCommand.decode(buffer);
        assertThat(decodeRequest.getExtFields().get("bname")).isEqualTo(brokerName);
        assertThat(decodeRequest.getExtFields().get("nsd")).isEqualTo(String.valueOf(namespaced));
        assertThat(decodeRequest.getExtFields().get("ns")).isEqualTo(namespace);
        assertThat(decodeRequest.getExtFields().get("oway")).isEqualTo(String.valueOf(oneway));
    }
}