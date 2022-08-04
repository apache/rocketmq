package org.apache.rocketmq.store.ha.netty;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;
import java.nio.ByteBuffer;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.store.ha.autoswitch.AutoSwitchHAService;

@ChannelHandler.Sharable
public class NettyTransferEncoder extends MessageToByteEncoder<TransferMessage> {

    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    private final AutoSwitchHAService autoSwitchHAService;

    public NettyTransferEncoder(AutoSwitchHAService autoSwitchHAService) {
        this.autoSwitchHAService = autoSwitchHAService;
    }

    @Override
    protected void encode(ChannelHandlerContext ctx, TransferMessage message, ByteBuf out) throws Exception {
        if (message == null || message.getType() == null) {
            String errorMessage = "The encode message is null or message type not exist";
            log.error(errorMessage);
            throw new RuntimeException(errorMessage);
        }

        out.writeInt(message.getBodyLength());
        out.writeInt(message.getType().getValue());
        out.writeLong(autoSwitchHAService.getCurrentMasterEpoch());
        out.writeLong(System.currentTimeMillis());
        for (ByteBuffer byteBuffer : message.getByteBufferList()) {
            out.writeBytes(byteBuffer);
        }
    }
}