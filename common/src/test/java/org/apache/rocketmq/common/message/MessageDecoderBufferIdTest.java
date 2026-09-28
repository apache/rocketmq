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

package org.apache.rocketmq.common.message;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class MessageDecoderBufferIdTest {
    @Test
    public void testIpv4IdWithLargerReusableBuffer() {
        assertMessageId(ByteBuffer.allocate(28));
    }

    @Test
    public void testIpv4IdWithDirectBuffer() {
        assertMessageId(ByteBuffer.allocateDirect(28));
    }

    @Test
    public void testIpv4IdWithSlicedBuffer() {
        ByteBuffer backing = ByteBuffer.allocate(48);
        backing.position(8);
        assertMessageId(backing.slice());
    }

    private void assertMessageId(ByteBuffer output) {
        InetSocketAddress address = new InetSocketAddress("127.0.0.1", 10911);
        String expected = MessageDecoder.createMessageId(address, 37);
        assertEquals(expected, MessageDecoder.createMessageId(output,
            MessageExt.socketAddress2ByteBuffer(address), 37));
    }
}
