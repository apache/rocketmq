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

package org.apache.rocketmq.remoting.common;

import org.apache.rocketmq.remoting.api.protocol.ProtocolFactory;
import org.apache.rocketmq.remoting.api.serializable.SerializerFactory;
import org.apache.rocketmq.remoting.impl.protocol.Httpv2Protocol;
import org.apache.rocketmq.remoting.impl.protocol.ProtocolFactoryImpl;
import org.apache.rocketmq.remoting.impl.protocol.serializer.MsgPackSerializer;
import org.apache.rocketmq.remoting.impl.protocol.serializer.SerializerFactoryImpl;

public class RemotingCommandFactoryMeta {
    private final ProtocolFactory protocolFactory = new ProtocolFactoryImpl();
    private final SerializerFactory serializerFactory = new SerializerFactoryImpl();
    private byte protocolType = Httpv2Protocol.MVP_MAGIC;
    private byte serializeType = MsgPackSerializer.SERIALIZER_TYPE;

    public RemotingCommandFactoryMeta() {
    }

    public RemotingCommandFactoryMeta(String protocolName, String serializeName) {
        this.protocolType = protocolFactory.type(protocolName);
        this.serializeType = serializerFactory.type(serializeName);
    }

    public byte getSerializeType() {
        return serializeType;
    }

    public byte getProtocolType() {
        return protocolType;
    }

}
