package org.apache.rocketmq.store.ha.netty;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import java.nio.ByteBuffer;
import java.util.List;
import org.apache.rocketmq.common.EpochEntry;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.common.RemotingUtil;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;
import org.apache.rocketmq.store.ha.autoswitch.AutoSwitchHAService;
import org.apache.rocketmq.store.ha.protocol.ConfirmTruncate;
import org.apache.rocketmq.store.ha.protocol.HandshakeMaster;
import org.apache.rocketmq.store.ha.protocol.HandshakeResult;
import org.apache.rocketmq.store.ha.protocol.HandshakeSlave;
import org.apache.rocketmq.store.ha.protocol.PushCommitLogAck;

public class NettyTransferServerHandler extends SimpleChannelInboundHandler<TransferMessage> {

    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    private final AutoSwitchHAService autoSwitchHAService;

    public NettyTransferServerHandler(AutoSwitchHAService autoSwitchHAService) {
        this.autoSwitchHAService = autoSwitchHAService;
    }

    @Override
    public void channelUnregistered(ChannelHandlerContext ctx) throws Exception {
        autoSwitchHAService.removeConnection(ctx.channel());
        super.channelUnregistered(ctx);
    }

    public void slaveHandshake(ChannelHandlerContext ctx, TransferMessage request) {
        HandshakeSlave handshakeSlave = RemotingSerializable.decode(request.getBytes(), HandshakeSlave.class);
        HandshakeResult handshakeResult = autoSwitchHAService.verifySlaveIdentity(handshakeSlave);
        autoSwitchHAService.tryAcceptNewSlave(ctx.channel(), handshakeSlave);
        HandshakeMaster handshakeMaster = autoSwitchHAService.buildHandshakeResult(handshakeResult);
        TransferMessage replyMessage = autoSwitchHAService.buildMessage(TransferType.HANDSHAKE_MASTER);
        replyMessage.appendBody(RemotingSerializable.encode(handshakeMaster));
        ctx.channel().writeAndFlush(replyMessage);
    }

    public void responseEpochList(ChannelHandlerContext ctx, TransferMessage request) {
        List<EpochEntry> entries = autoSwitchHAService.getEpochEntries();
        // Set epoch end offset == message store max offset
        if (entries.size() > 0) {
            entries.get(entries.size() - 1).setEndOffset(autoSwitchHAService.getDefaultMessageStore().getMaxPhyOffset());
        }
        ByteBuffer byteBuffer = ByteBuffer.allocate(3 * 8 * entries.size());
        for (EpochEntry entry : entries) {
            byteBuffer.putLong(entry.getEpoch()).putLong(entry.getStartOffset()).putLong(entry.getEndOffset());
        }
        byteBuffer.flip();
        TransferMessage replyMessage = autoSwitchHAService.buildMessage(TransferType.RETURN_EPOCH);
        replyMessage.appendBody(byteBuffer);
        ctx.channel().writeAndFlush(replyMessage);
    }

    /**
     * Master change state to transfer and start push data to slave
     */
    public void confirmTruncate(ChannelHandlerContext ctx, TransferMessage message) {
        ConfirmTruncate confirmTruncate = RemotingSerializable.decode(message.getBytes(), ConfirmTruncate.class);
        autoSwitchHAService.confirmTruncate(ctx.channel(), confirmTruncate);
    }

    public void pushCommitLogAck(ChannelHandlerContext ctx, TransferMessage message) {
        PushCommitLogAck pushCommitLogAck = RemotingSerializable.decode(message.getBytes(), PushCommitLogAck.class);
        autoSwitchHAService.pushCommitLogDataAck(ctx.channel(), pushCommitLogAck);
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, TransferMessage request) {

        if (ctx != null) {
            log.debug("receive request, {} {} {}",
                request != null ? request.getType() : "unknown",
                RemotingHelper.parseChannelRemoteAddr(ctx.channel()),
                request);
        } else {
            return;
        }

        if (request == null || request.getType() == null) {
            log.error("receive empty request");
            return;
        }

        if (!autoSwitchHAService.isSlaveEpochMatchMaster(request.getEpoch())) {
            log.info("receive empty request, epoch not match, connection epoch:{}", request.getEpoch());
            RemotingUtil.closeChannel(ctx.channel());
            return;
        }

        switch (request.getType()) {
            case HANDSHAKE_SLAVE:
                this.slaveHandshake(ctx, request);
                break;
            case QUERY_EPOCH:
                this.responseEpochList(ctx, request);
                break;
            case CONFIRM_TRUNCATE:
                confirmTruncate(ctx, request);
                break;
            case TRANSFER_ACK:
                pushCommitLogAck(ctx, request);
                break;
            default:
                log.error("receive request type {} not supported", request.getType());
        }
    }
}
