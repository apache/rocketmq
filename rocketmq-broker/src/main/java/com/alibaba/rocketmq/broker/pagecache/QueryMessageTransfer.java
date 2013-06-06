/**
 * $Id: QueryMessageTransfer.java 1831 2013-05-16 01:39:51Z shijia.wxr $
 */
package com.alibaba.rocketmq.broker.pagecache;

import io.netty.buffer.AbstractReferenceCounted;
import io.netty.channel.FileRegion;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.List;

import com.alibaba.rocketmq.store.QueryMessageResult;


/**
 * @author shijia.wxr<vintage.wang@gmail.com>
 * 
 */
public class QueryMessageTransfer extends AbstractReferenceCounted implements FileRegion {
    private final ByteBuffer byteBufferHeader;
    private final QueryMessageResult queryMessageResult;


    public QueryMessageTransfer(ByteBuffer byteBufferHeader, QueryMessageResult queryMessageResult) {
        this.byteBufferHeader = byteBufferHeader;
        this.queryMessageResult = queryMessageResult;
    }


    @Override
    public long position() {
        int pos = byteBufferHeader.position();
        List<ByteBuffer> messageBufferList = this.queryMessageResult.getMessageBufferList();
        for (ByteBuffer bb : messageBufferList) {
            pos += bb.position();
        }
        return pos;
    }


    @Override
    public long count() {
        return byteBufferHeader.limit() + this.queryMessageResult.getBufferTotalSize();
    }


    @Override
    public long transferTo(WritableByteChannel target, long position) throws IOException {
        if (this.byteBufferHeader.hasRemaining()) {
            return target.write(this.byteBufferHeader);
        }
        else {
            List<ByteBuffer> messageBufferList = this.queryMessageResult.getMessageBufferList();
            for (ByteBuffer bb : messageBufferList) {
                if (bb.hasRemaining()) {
                    return target.write(bb);
                }
            }
        }

        return 0;
    }


    public void close() {
        this.deallocate();
    }


    @Override
    protected void deallocate() {
        this.queryMessageResult.release();
    }
}
