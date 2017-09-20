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

package org.apache.rocketmq.remoting.api.buffer;

import java.nio.ByteBuffer;

public interface ByteBufferWrapper {
    void writeByte(byte data);

    void writeByte(int index, byte data);

    void writeBytes(byte[] data);

    void writeBytes(ByteBuffer data);

    void writeInt(int data);

    void writeShort(short value);

    void writeLong(long id);

    byte readByte();

    void readBytes(byte[] dst);

    void readBytes(ByteBuffer dst);

    short readShort();

    int readInt();

    long readLong();

    int readableBytes();

    int readerIndex();

    void setReaderIndex(int readerIndex);

    void ensureCapacity(int capacity);
}
