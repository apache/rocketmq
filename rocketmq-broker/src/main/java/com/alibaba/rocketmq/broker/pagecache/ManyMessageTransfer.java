/**
 * $Id: ManyMessageTransfer.java 1831 2013-05-16 01:39:51Z shijia.wxr $
 */
package com.alibaba.rocketmq.broker.pagecache;

import io.netty.channel.FileRegion;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.List;

import com.alibaba.rocketmq.store.GetMessageResult;
import io.netty.util.AbstractReferenceCounted;


/**
 * @author shijia.wxr<vintage.wang@gmail.com>
 * 
 */
public class ManyMessageTransfer extends AbstractReferenceCounted implements FileRegion {
    private final ByteBuffer byteBufferHeader;
    private final GetMessageResult getMessageResult;
    private long transfered ; //the bytes which was transfered already

    public ManyMessageTransfer(ByteBuffer byteBufferHeader, GetMessageResult getMessageResult) {
        this.byteBufferHeader = byteBufferHeader;
        this.getMessageResult = getMessageResult;
    }


    @Override
    public long position() {
        int pos = byteBufferHeader.position();
        List<ByteBuffer> messageBufferList = this.getMessageResult.getMessageBufferList();
        for (ByteBuffer bb : messageBufferList) {
            pos += bb.position();
        }
        return pos;
    }


    @Override
    public long count() {
        return byteBufferHeader.limit() + this.getMessageResult.getBufferTotalSize();
    }


    @Override
    public long transferTo(WritableByteChannel target, long position) throws IOException {
        if (this.byteBufferHeader.hasRemaining()) {
            transfered +=  target.write(this.byteBufferHeader);
            return transfered;
        }
        else {
            List<ByteBuffer> messageBufferList = this.getMessageResult.getMessageBufferList();
            for (ByteBuffer bb : messageBufferList) {
                if (bb.hasRemaining()) {
                    transfered +=  target.write(bb);
                    return transfered;
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

    @Override
    public long transfered() {
        return transfered;
    }
}
