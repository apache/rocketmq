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
package org.apache.rocketmq.store.jna;

import com.sun.jna.Pointer;
import org.apache.rocketmq.store.util.JNASdk;
import org.junit.Test;
import sun.nio.ch.DirectBuffer;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class JnaSdkTest {

    @Test
    public void mlockTest() {
        List<ByteBuffer> buffers = new ArrayList<>();
        int size = 1024 * 1024 * 1024; // 1G
        for (int i = 0; i < 3; i++) {
            ByteBuffer byteBuffer = ByteBuffer.allocateDirect(1024 * 1024 * 1024);
            buffers.add(byteBuffer);

            final long address = ((DirectBuffer) byteBuffer).address();
            int locked = JNASdk.mlock(new Pointer(address), size);
            assertThat(locked).isEqualTo(0);
        }

        for (ByteBuffer buffer : buffers) {
            final long address = ((DirectBuffer) buffer).address();
            int unlocked = JNASdk.munlock(new Pointer(address), size);
            assertThat(unlocked).isEqualTo(0);
        }
    }

    @Test
    public void mlockFailTest() {
        int size = 1024 * 1024 * 1024; // 1G
        ByteBuffer byteBuffer = ByteBuffer.allocateDirect(10);
        final long address = ((DirectBuffer) byteBuffer).address();

        int unlocked = JNASdk.munlock(new Pointer(address), size);
        assertThat(unlocked).isEqualTo(-1);
    }
}
