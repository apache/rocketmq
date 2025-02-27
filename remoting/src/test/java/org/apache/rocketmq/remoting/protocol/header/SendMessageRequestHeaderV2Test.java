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

package org.apache.rocketmq.remoting.protocol.header;

import java.nio.ByteBuffer;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.RequestCode;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class SendMessageRequestHeaderV2Test {
    SendMessageRequestHeaderV2 header = new SendMessageRequestHeaderV2();
    String topic = "test";
    int queueId = 5;

    @Test
    public void testEncodeDecode() throws RemotingCommandException {
        header.setQueueId(queueId);
        header.setTopic(topic);

        RemotingCommand remotingCommand = RemotingCommand.createRequestCommand(RequestCode.SEND_MESSAGE_V2, header);
        ByteBuffer buffer = remotingCommand.encode();

        //Simulate buffer being read in NettyDecoder
        buffer.getInt();
        byte[] bytes = new byte[buffer.limit() - 4];
        buffer.get(bytes, 0, buffer.limit() - 4);
        buffer = ByteBuffer.wrap(bytes);

        RemotingCommand decodeRequest = RemotingCommand.decode(buffer);
        assertThat(decodeRequest.getExtFields().get("e")).isEqualTo(String.valueOf(queueId));
        assertThat(decodeRequest.getExtFields().get("b")).isEqualTo(topic);
    }
}