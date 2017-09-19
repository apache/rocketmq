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

package org.apache.rocketmq.remoting.impl.protocol;

import io.netty.handler.ssl.SslContext;
import org.apache.rocketmq.remoting.api.protocol.Protocol;
import org.apache.rocketmq.remoting.api.protocol.ProtocolFactory;

public class ProtocolFactoryImpl implements ProtocolFactory {
    private static final int MAX_COUNT = 0x0FF;
    private final Protocol[] tables = new Protocol[MAX_COUNT];

    private SslContext sslContext;

    public ProtocolFactoryImpl(final SslContext sslContext) {
        this.sslContext = sslContext;
        this.register(new RemotingCoreProtocol());
        this.register(new Httpv2Protocol(sslContext));
        this.register(new WebSocketProtocol());
    }

    public ProtocolFactoryImpl() {
        this.register(new RemotingCoreProtocol());
        this.register(new Httpv2Protocol(sslContext));
        this.register(new WebSocketProtocol());
    }

    @Override
    public void register(Protocol protocol) {
        if (tables[protocol.type() & MAX_COUNT] != null) {
            throw new RuntimeException("protocol header's sign is overlapped");
        }
        tables[protocol.type() & MAX_COUNT] = protocol;
    }

    @Override
    public void resetAll(final Protocol protocol) {
        for (int i = 0; i < MAX_COUNT; i++) {
            tables[i] = protocol;
        }
    }

    @Override
    public byte type(final String protocolName) {

        for (int i = 0; i < this.tables.length; i++) {
            if (this.tables[i] != null) {
                if (this.tables[i].name().equalsIgnoreCase(protocolName)) {
                    return this.tables[i].type();
                }
            }
        }

        throw new IllegalArgumentException(String.format("the protocol: %s not exist", protocolName));
    }

    @Override
    public Protocol get(byte type) {
        return tables[type & MAX_COUNT];
    }

    @Override
    public void clearAll() {
        for (int i = 0; i < this.tables.length; i++) {
            this.tables[i] = null;
        }
    }
}
