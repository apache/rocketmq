/**
 * $Id: ManyMessageTransfer.java 1831 2013-05-16 01:39:51Z shijia.wxr $
 */
package com.alibaba.rocketmq.broker.pagecache;

import io.netty.buffer.AbstractReferenceCounted;
import io.netty.channel.FileRegion;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.List;

import com.alibaba.rocketmq.store.GetMessageResult;


/**
 * @author vintage.wang@gmail.com shijia.wxr@taobao.com
 * 
 */
public class ManyMessageTransfer extends AbstractReferenceCounted implements FileRegion {
    private final ByteBuffer byteBufferHeader;
    private final GetMessageResult getMessageResult;


    public ManyMessageTransfer(ByteBuffer byteBufferHeader, GetMessageResult getMessageResult) {
        this.byteBufferHeader = byteBufferHeader;
        this.getMessageResult = getMessageResult;
    }


    @Override
    public long position() {
        List<ByteBuffer> messageBufferList = this.getMessageResult.getMessageBufferList();
        int pos = 0;
        for (ByteBuffer bb : messageBufferList) {
            pos += bb.position();
        }
        return pos;
    }


    @Override
    public long count() {
        return this.getMessageResult.getBufferTotalSize();
    }


    @Override
    public long transferTo(WritableByteChannel target, long position) throws IOException {
        if (this.byteBufferHeader.hasRemaining()) {
            return target.write(this.byteBufferHeader);
        }
        else {
            List<ByteBuffer> messageBufferList = this.getMessageResult.getMessageBufferList();
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
        this.getMessageResult.release();
    }
}
