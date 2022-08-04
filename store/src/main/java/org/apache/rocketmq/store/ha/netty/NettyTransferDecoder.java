package org.apache.rocketmq.store.ha.netty;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.common.RemotingUtil;
import org.apache.rocketmq.store.ha.autoswitch.AutoSwitchHAService;

public class NettyTransferDecoder extends LengthFieldBasedFrameDecoder {

    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    private static final int FRAME_MAX_LENGTH =
        Integer.parseInt(System.getProperty("com.rocketmq.remoting.frameMaxLength", "16777216"));

    private final AutoSwitchHAService autoSwitchHAService;

    public NettyTransferDecoder(AutoSwitchHAService autoSwitchHAService) {
        super(FRAME_MAX_LENGTH, 0, 4, 20, 0);
        this.autoSwitchHAService = autoSwitchHAService;
    }

    @Override
    protected TransferMessage decode(ChannelHandlerContext ctx, ByteBuf in) throws Exception {
        try {
            ByteBuf frame = (ByteBuf) super.decode(ctx, in);
            if (null == frame) {
                return null;
            }
            int bodyLength = frame.readInt();
            TransferType messageType = TransferType.valueOf(frame.readInt());
            long epoch = frame.readLong();
            long lastReadTimestamp = frame.readLong();
            byte[] body = new byte[bodyLength];
            frame.readBytes(body);
            //System.out.println(messageType);
            TransferMessage request = new TransferMessage(messageType, epoch);
            request.appendBody(body);
            return request;
        } catch (Exception e) {
            log.error("Decode exception, " + RemotingHelper.parseChannelRemoteAddr(ctx.channel()), e);
            RemotingUtil.closeChannel(ctx.channel());
        }
        return null;
    }
}
