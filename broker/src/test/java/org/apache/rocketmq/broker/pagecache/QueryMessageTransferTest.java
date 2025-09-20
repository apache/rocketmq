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
package org.apache.rocketmq.broker.pagecache;

import org.apache.rocketmq.store.QueryMessageResult;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class QueryMessageTransferTest {

    @Mock
    private WritableByteChannel writableByteChannel;

    @Mock
    private QueryMessageResult queryMessageResult;

    private QueryMessageTransfer queryMessageTransfer;

    private ByteBuffer byteBufferHeader;

    private ByteBuffer bb1;

    private ByteBuffer bb2;

    @Before
    public void init() {
        byteBufferHeader = ByteBuffer.allocate(4);
        byteBufferHeader.putInt(1);
        byteBufferHeader.flip();

        bb1 = ByteBuffer.allocate(4);
        bb1.putInt(2);
        bb1.flip();

        bb2 = ByteBuffer.allocate(4);
        bb2.putInt(3);
        bb2.flip();

        when(queryMessageResult.getMessageBufferList()).thenReturn(Arrays.asList(bb1, bb2));

        queryMessageTransfer = new QueryMessageTransfer(byteBufferHeader, queryMessageResult);
    }

    @Test
    public void testPosition_WithHeaderAndMessageBuffers() {
        byteBufferHeader.position(2);
        bb1.position(1);
        bb2.position(3);

        long actual = queryMessageTransfer.position();

        long expected = byteBufferHeader.position() + bb1.position() + bb2.position();
        assertEquals(expected, actual);
    }

    @Test
    public void testPosition_WithHeaderOnly() {
        byteBufferHeader.position(2);

        when(queryMessageResult.getMessageBufferList()).thenReturn(new ArrayList<>());

        long actual = queryMessageTransfer.position();

        long expected = byteBufferHeader.position();
        assertEquals(expected, actual);
    }

    @Test
    public void testPosition_WithMessageBuffersOnly() {
        byteBufferHeader.clear();
        byteBufferHeader.flip();

        bb1.position(1);
        bb2.position(3);

        long actual = queryMessageTransfer.position();

        long expected = bb1.position() + bb2.position();
        assertEquals(expected, actual);
    }

    @Test
    public void testTransferTo_OnlyHeaderData() throws Exception {
        bb1.clear();
        bb2.clear();

        when(writableByteChannel.write(byteBufferHeader)).thenReturn(4);

        long actual = queryMessageTransfer.transferTo(writableByteChannel, 0);

        assertEquals(4, actual);
        verify(writableByteChannel, times(1)).write(byteBufferHeader);
        verify(writableByteChannel, never()).write(bb1);
        verify(writableByteChannel, never()).write(bb2);
    }

    @Test
    public void testTransferTo_OnlyMessageBuffersData() throws Exception {
        byteBufferHeader.clear();
        byteBufferHeader.flip();

        when(writableByteChannel.write(bb1)).thenReturn(4);

        long actual = queryMessageTransfer.transferTo(writableByteChannel, 0);

        assertEquals(4, actual);
        verify(writableByteChannel, never()).write(byteBufferHeader);
        verify(writableByteChannel, times(1)).write(bb1);
    }
}
