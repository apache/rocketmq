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

package org.apache.rocketmq.remoting.impl.channel;

import io.netty.channel.FileRegion;
import io.netty.util.AbstractReferenceCounted;
import java.io.IOException;
import java.nio.channels.WritableByteChannel;
import org.apache.rocketmq.remoting.api.channel.ChunkRegion;

public class FileRegionImpl extends AbstractReferenceCounted implements FileRegion {
    private final ChunkRegion chunkRegion;

    public FileRegionImpl(ChunkRegion chunkRegion) {
        this.chunkRegion = chunkRegion;
    }

    @Override
    public long position() {
        return chunkRegion.position();
    }

    @Override
    public long transfered() {
        return chunkRegion.transferred();
    }

    @Override
    public long transferred() {
        return chunkRegion.transferred();
    }

    @Override
    public long count() {
        return chunkRegion.count();
    }

    @Override
    public long transferTo(WritableByteChannel target, long position) throws IOException {
        return chunkRegion.transferTo(target, position);
    }

    @Override
    protected void deallocate() {
        chunkRegion.release();
    }

    @Override
    public FileRegion retain() {
        super.retain();
        return this;
    }

    @Override
    public FileRegion retain(int increment) {
        super.retain(increment);
        return this;
    }

    @Override
    public FileRegion touch() {
        return this;
    }

    @Override
    public FileRegion touch(Object hint) {
        return this;
    }

}
