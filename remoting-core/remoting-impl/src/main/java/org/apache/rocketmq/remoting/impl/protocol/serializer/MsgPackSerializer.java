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

package org.apache.rocketmq.remoting.impl.protocol.serializer;

import java.lang.reflect.Type;
import java.nio.ByteBuffer;
import org.apache.rocketmq.remoting.api.serializable.Serializer;
import org.apache.rocketmq.remoting.common.TypePresentation;
import org.msgpack.MessagePack;
import org.msgpack.template.Template;

public class MsgPackSerializer implements Serializer {
    public static final String SERIALIZER_NAME = MsgPackSerializer.class.getSimpleName();
    public static final byte SERIALIZER_TYPE = 'M';
    private final MessagePack messagePack = new MessagePack();

    public MsgPackSerializer() {
    }

    @Override
    public String name() {
        return SERIALIZER_NAME;
    }

    @Override
    public byte type() {
        return SERIALIZER_TYPE;
    }

    @Override
    public <T> T decode(final byte[] content, final Class<T> c) {
        try {
            return messagePack.read(content, c);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public <T> T decode(final byte[] content, final TypePresentation<T> typePresentation) {
        return decode(content, typePresentation.getType());
    }

    @Override
    public <T> T decode(byte[] content, Type type) {
        Template<T> template = (Template<T>) messagePack.lookup(type);
        try {
            return messagePack.read(content, template);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public ByteBuffer encode(final Object object) {
        try {
            byte[] data = messagePack.write(object);
            return ByteBuffer.wrap(data);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
