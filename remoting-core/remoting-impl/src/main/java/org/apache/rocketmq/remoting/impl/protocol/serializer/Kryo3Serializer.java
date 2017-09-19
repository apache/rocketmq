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

import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.nio.ByteBuffer;
import org.apache.rocketmq.remoting.api.serializable.Serializer;
import org.apache.rocketmq.remoting.common.TypePresentation;

public class Kryo3Serializer implements Serializer {
    public static final String SERIALIZER_NAME = Kryo3Serializer.class.getSimpleName();
    public static final byte SERIALIZER_TYPE = 'K';

    public Kryo3Serializer() {
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
        if (content != null) {
            Input input = null;
            try {
                input = new Input(content);
                return (T) ThreadSafeKryo.getKryoInstance().readClassAndObject(input);
            } catch (Exception e) {
                throw new RuntimeException(e);
            } finally {
                input.close();
            }
        }

        return null;
    }

    @Override
    public <T> T decode(final byte[] content, final TypePresentation<T> typePresentation) {
        return decode(content, typePresentation.getType());
    }

    @Override
    public <T> T decode(byte[] content, Type type) {
        if (type instanceof ParameterizedType) {
            return decode(content, (Class<? extends T>) ((ParameterizedType) type).getRawType());
        } else if (type instanceof Class) {
            return decode(content, (Class<? extends T>) type);
        }
        return null;
    }

    @Override
    public ByteBuffer encode(final Object object) {
        if (object != null) {
            try (Output output = new Output(1024, 1024 * 1024 * 6)) {
                ThreadSafeKryo.getKryoInstance().writeClassAndObject(output, object);
                return ByteBuffer.wrap(output.getBuffer(), 0, output.position());
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        return null;
    }
}
