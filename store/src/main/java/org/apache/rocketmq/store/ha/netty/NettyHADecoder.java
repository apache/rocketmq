package org.apache.rocketmq.store.ha.netty;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.common.RemotingUtil;

public class NettyHADecoder extends LengthFieldBasedFrameDecoder {

    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    private static final int FRAME_MAX_LENGTH =
        Integer.parseInt(System.getProperty("com.rocketmq.remoting.frameMaxLength", "16777216"));

    private long lastReadTimestamp = System.currentTimeMillis();

    public NettyHADecoder() {
        super(FRAME_MAX_LENGTH, 0, 4, 20, 0);
    }

    public long getLastReadTimestamp() {
        return lastReadTimestamp;
    }

    @Override
    protected HAMessage decode(ChannelHandlerContext ctx, ByteBuf in) throws Exception {
        ByteBuf frame = null;
        try {
            frame = (ByteBuf) super.decode(ctx, in);
            if (null == frame) {
                return null;
            }

            int bodyLength = frame.readInt();
            HAMessageType messageType = HAMessageType.valueOf(frame.readInt());
            long epoch = frame.readLong();
            this.lastReadTimestamp = frame.readLong();

            // if direct byte buffer, return underlying nioBuffer.
            // otherwise copy to a byte array and wrap it.
            //ByteBuffer byteBuffer;
            //if (frame.isDirect()) {
            //    byteBuffer = frame.nioBuffer();
            //} else {
            //    final byte[] out = new byte[frame.readableBytes()];
            //    frame.getBytes(frame.readerIndex(), out, 0, bodyLength);
            //    byteBuffer = ByteBuffer.wrap(out);
            //}

            byte[] body = new byte[bodyLength];
            frame.readBytes(body);
            return new HAMessage(messageType, epoch, body);

        } catch (Exception e) {
            System.out.println(e.toString());
            log.error("decode exception, " + RemotingHelper.parseChannelRemoteAddr(ctx.channel()), e);
            RemotingUtil.closeChannel(ctx.channel());
        } finally {
            //if (null != frame) {
            //    frame.release();
            //}
        }
        return null;
    }
}
