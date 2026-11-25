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
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class RpcClientUtilsTest {

    @Test
    public void testEncodeBodyByteBufferPreservesCallerMarkAndPosition() {
        ByteBuffer buffer = ByteBuffer.allocate(5);
        buffer.put(new byte[] {1, 2, 3, 4, 5});
        buffer.flip();
        buffer.position(1);
        buffer.mark();
        buffer.position(3);

        byte[] data = RpcClientUtils.encodeBody(buffer);

        assertThat(data).hasSize(2);
        assertThat(data).containsExactly((byte) 4, (byte) 5);
        assertThat(buffer.position()).isEqualTo(3);
        assertThat(buffer.limit()).isEqualTo(5);
        // Regression for issue #11283: the caller's mark must survive encodeBody,
        // so reset() rewinds to the marked position instead of the current one.
        buffer.reset();
        assertThat(buffer.position()).isEqualTo(1);
    }

    @Test
    public void testEncodeBodyByteBufferReadsRemainingBytes() {
        ByteBuffer buffer = ByteBuffer.allocate(5);
        buffer.put(new byte[] {1, 2, 3, 4, 5});
        buffer.flip();

        byte[] data = RpcClientUtils.encodeBody(buffer);

        assertThat(data).containsExactly((byte) 1, (byte) 2, (byte) 3, (byte) 4, (byte) 5);
        assertThat(buffer.position()).isEqualTo(0);
        assertThat(buffer.limit()).isEqualTo(5);
    }

    @Test
    public void testEncodeBodyByteBufferReadOnly() {
        ByteBuffer buffer = ByteBuffer.allocate(5);
        buffer.put(new byte[] {1, 2, 3, 4, 5});
        buffer.flip();
        buffer.position(2);
        ByteBuffer readOnlyBuffer = buffer.asReadOnlyBuffer();

        byte[] data = RpcClientUtils.encodeBody(readOnlyBuffer);

        assertThat(data).containsExactly((byte) 3, (byte) 4, (byte) 5);
        assertThat(readOnlyBuffer.position()).isEqualTo(2);
        assertThat(readOnlyBuffer.limit()).isEqualTo(5);
    }

    @Test
    public void testEncodeBodyNullReturnsNull() {
        assertThat(RpcClientUtils.encodeBody(null)).isNull();
    }

    @Test
    public void testEncodeBodyByteArrayPassthrough() {
        byte[] body = new byte[] {1, 2, 3};

        assertThat(RpcClientUtils.encodeBody(body)).isSameAs(body);
    }
}
