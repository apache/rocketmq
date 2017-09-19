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

package org.apache.rocketmq.remoting.impl.protocol.compression;

import org.apache.rocketmq.remoting.api.compressable.Compressor;
import org.apache.rocketmq.remoting.api.compressable.CompressorFactory;

public class CompressorFactoryImpl implements CompressorFactory {
    private static final int MAX_COUNT = 0x0FF;
    private final Compressor[] tables = new Compressor[MAX_COUNT];

    public CompressorFactoryImpl() {
        this.register(new GZipCompressor());
    }

    @Override
    public void register(Compressor compressor) {
        if (tables[compressor.type() & MAX_COUNT] != null) {
            throw new RuntimeException("compressor header's sign is overlapped");
        }
        tables[compressor.type() & MAX_COUNT] = compressor;
    }

    @Override
    public byte type(String compressionName) {
        for (Compressor table : this.tables) {
            if (table != null) {
                if (table.name().equalsIgnoreCase(compressionName)) {
                    return table.type();
                }
            }
        }

        throw new IllegalArgumentException(String.format("the compressor: %s not exist", compressionName));

    }

    @Override
    public Compressor get(byte type) {
        return tables[type & MAX_COUNT];
    }

    @Override
    public void clearAll() {
        for (int i = 0; i < this.tables.length; i++) {
            this.tables[i] = null;
        }
    }

    public Compressor[] getTables() {
        return tables;
    }
}
