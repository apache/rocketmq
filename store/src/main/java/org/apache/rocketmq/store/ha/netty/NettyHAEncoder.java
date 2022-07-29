package org.apache.rocketmq.store.ha.netty;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;

import java.nio.ByteBuffer;

@ChannelHandler.Sharable
public class NettyHAEncoder extends MessageToByteEncoder<HAMessage> {

    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    private long lastWriteTimestamp = System.currentTimeMillis();

    public long getLastWriteTimestamp() {
        return lastWriteTimestamp;
    }

    @Override
    protected void encode(ChannelHandlerContext ctx, HAMessage message, ByteBuf out) throws Exception {
        if (message == null || message.getType() == null) {
            throw new RuntimeException("The encode message is null");
        }

        this.lastWriteTimestamp = System.currentTimeMillis();

        out.writeInt(message.getBodyLength());
        out.writeInt(message.getType().getValue());
        out.writeLong(message.getEpoch());
        out.writeLong(this.lastWriteTimestamp);
        if (message.getBodyLength() > 0) {
            for (ByteBuffer byteBuffer : message.getByteBufferList()) {
                out.writeBytes(byteBuffer);
            }
        }
    }
}