/**
 * $Id: OneMessageTransfer.java 1831 2013-05-16 01:39:51Z shijia.wxr $
 */
package com.alibaba.rocketmq.broker.pagecache;

import io.netty.buffer.AbstractReferenceCounted;
import io.netty.channel.FileRegion;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;

import com.alibaba.rocketmq.store.SelectMapedBufferResult;


/**
 * @author shijia.wxr<vintage.wang@gmail.com>
 * 
 */
public class OneMessageTransfer extends AbstractReferenceCounted implements FileRegion {
    private final ByteBuffer byteBufferHeader;
    private final SelectMapedBufferResult selectMapedBufferResult;


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
            return target.write(this.byteBufferHeader);
        }
        else if (this.selectMapedBufferResult.getByteBuffer().hasRemaining()) {
            return target.write(this.selectMapedBufferResult.getByteBuffer());
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
}
