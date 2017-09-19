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

import org.apache.rocketmq.remoting.api.serializable.Serializer;
import org.apache.rocketmq.remoting.api.serializable.SerializerFactory;

public class SerializerFactoryImpl implements SerializerFactory {
    private static final int MAX_COUNT = 0x0FF;
    private final Serializer[] tables = new Serializer[MAX_COUNT];

    public SerializerFactoryImpl() {
        this.register(new JsonSerializer());
        this.register(new Kryo3Serializer());
        this.register(new MsgPackSerializer());
    }

    @Override
    public void register(Serializer serialization) {
        if (tables[serialization.type() & MAX_COUNT] != null) {
            throw new RuntimeException("serialization header's sign is overlapped");
        }
        tables[serialization.type() & MAX_COUNT] = serialization;
    }

    @Override
    public byte type(final String serializationName) {
        for (Serializer table : this.tables) {
            if (table != null) {
                if (table.name().equalsIgnoreCase(serializationName)) {
                    return table.type();
                }
            }
        }

        throw new IllegalArgumentException(String.format("the serialization: %s not exist", serializationName));
    }

    @Override
    public Serializer get(byte type) {
        return tables[type & MAX_COUNT];
    }

    @Override
    public void clearAll() {
        for (int i = 0; i < this.tables.length; i++) {
            this.tables[i] = null;
        }
    }

    public Serializer[] getTables() {
        return tables;
    }
}
