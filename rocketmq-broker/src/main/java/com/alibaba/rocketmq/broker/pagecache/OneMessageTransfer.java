/**
 * Copyright (C) 2010-2013 Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.rocketmq.broker.pagecache;

import io.netty.channel.FileRegion;
import io.netty.util.AbstractReferenceCounted;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;

import com.alibaba.rocketmq.store.SelectMapedBufferResult;


/**
 * @author shijia.wxr<vintage.wang@gmail.com>
 * @since 2013-7-26
 */
public class OneMessageTransfer extends AbstractReferenceCounted implements FileRegion {
    private final ByteBuffer byteBufferHeader;
    private final SelectMapedBufferResult selectMapedBufferResult;
    private long transfered; // the bytes which was transfered already


    public OneMessageTransfer(ByteBuffer byteBufferHeader, SelectMapedBufferResult selectMapedBufferResult) {
        this.byteBufferHeader = byteBufferHeader;
        this.selectMapedBufferResult = selectMapedBufferResult;
    }


    @Override
    public long position() {
        return this.byteBufferHeader.position() + this.selectMapedBufferResult.getByteBuffer().position();
    }


    @Override
    public long count() {
        return this.byteBufferHeader.limit() + this.selectMapedBufferResult.getSize();
    }


    @Override
    public long transferTo(WritableByteChannel target, long position) throws IOException {
        if (this.byteBufferHeader.hasRemaining()) {
            transfered += target.write(this.byteBufferHeader);
            return transfered;
        }
        else if (this.selectMapedBufferResult.getByteBuffer().hasRemaining()) {
            transfered += target.write(this.selectMapedBufferResult.getByteBuffer());
            return transfered;
        }

        return 0;
    }


    public void close() {
        this.deallocate();
    }


    @Override
    protected void deallocate() {
        this.selectMapedBufferResult.release();
    }


    @Override
    public long transfered() {
        return transfered;
    }
}
