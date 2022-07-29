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

import io.netty.channel.FileRegion;
import io.netty.util.AbstractReferenceCounted;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Zero copy response for Batch-Protocol.
 *
 * The following diagram shows how a zero-copy response is constructed and which parts are zero-copy zones.
 *
 * Batch Packet Size,
 * Batch Header Length,
 * Batch Header,
 * Body : [
 *      Child1 Packet Size,
 *      Child1 Header Length,
 *      Child1 Header,
 *      Child1 Body, (zero-copy zone)
 *      Child2 Packet Size,
 *      Child2 Header Length,
 *      Child2 Header,
 *      Child2 Body, (zero-copy zone)
 *      Child3 Packet Size,
 *      Child3 Header Length,
 *      Child3 Header,
 *      Child3 Body  (zero-copy zone)
 * ]
 */
public class BatchManyMessageTransfer extends AbstractReferenceCounted implements FileRegion {
    // batch-header
    private final ByteBuffer batchHeader;
    // batch-body
    private final List<ManyMessageTransfer> manyMessageTransferList;

    private long headerTransferred = 0;
    private Map<ManyMessageTransfer, Long> bodyTransferred = new HashMap<>();

    public BatchManyMessageTransfer(ByteBuffer batchHeader, List<ManyMessageTransfer> manyMessageTransferList) {
        this.batchHeader = batchHeader;
        this.manyMessageTransferList = manyMessageTransferList;
    }

    @Override
    public long position() {
        return batchHeader.position() + manyMessageTransferList
                .stream()
                .mapToLong(ManyMessageTransfer::position)
                .sum();
    }

    @Override
    public long transfered() {
        return headerTransferred + this.bodyTransferred.values().stream().mapToLong(x -> x).sum();
    }

    @Override
    public long transferred() {
        return headerTransferred + this.bodyTransferred.values().stream().mapToLong(x -> x).sum();
    }

    @Override
    public long count() {
        return batchHeader.limit() + manyMessageTransferList
                .stream()
                .mapToLong(ManyMessageTransfer::count)
                .sum();
    }

    @Override
    public long transferTo(WritableByteChannel target, long position) throws IOException {
        if (this.batchHeader.hasRemaining()) {
            // batch-header
            headerTransferred += target.write(this.batchHeader);
            return headerTransferred;
        } else {
            // batch-body
            for (ManyMessageTransfer manyMessageTransfer : manyMessageTransferList) {
                if (!manyMessageTransfer.isComplete()) {
                    this.bodyTransferred.put(manyMessageTransfer, manyMessageTransfer.transferTo(target, position));
                    return headerTransferred + this.bodyTransferred.values().stream().mapToLong(x -> x).sum();
                }
            }
        }

        return 0;
    }

    @Override
    protected void deallocate() {
        manyMessageTransferList.forEach(ManyMessageTransfer::deallocate);
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

    public void close() {
        manyMessageTransferList.forEach(ManyMessageTransfer::close);
    }
}
