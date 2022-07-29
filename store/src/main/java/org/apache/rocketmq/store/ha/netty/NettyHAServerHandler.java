package org.apache.rocketmq.store.ha.netty;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.apache.rocketmq.common.EpochEntry;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.remoting.common.RemotingUtil;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;
import org.apache.rocketmq.store.ha.autoswitch.AutoSwitchHAService;
import org.apache.rocketmq.store.ha.protocol.ConfirmTruncate;
import org.apache.rocketmq.store.ha.protocol.HandshakeMaster;
import org.apache.rocketmq.store.ha.protocol.HandshakeResult;
import org.apache.rocketmq.store.ha.protocol.HandshakeSlave;
import org.apache.rocketmq.store.ha.protocol.PushCommitLogAck;

import java.nio.ByteBuffer;
import java.util.List;

public class NettyHAServerHandler extends SimpleChannelInboundHandler<HAMessage> {

    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    private final AutoSwitchHAService nettyHAService;

    public NettyHAServerHandler(AutoSwitchHAService nettyHAService) {
        this.nettyHAService = nettyHAService;
    }

    @Override
    public void channelUnregistered(ChannelHandlerContext ctx) throws Exception {
        nettyHAService.removeConnection(ctx.channel());
        super.channelUnregistered(ctx);
    }

    public void slaveHandshake(HAMessage message, Channel channel) {
        HandshakeSlave handshakeSlave = RemotingSerializable.decode(message.getBytes(), HandshakeSlave.class);
        HandshakeResult handshakeResult = nettyHAService.verifySlaveIdentity(handshakeSlave);
        nettyHAService.tryAcceptNewSlave(channel, handshakeSlave);
        HandshakeMaster handshakeMaster = nettyHAService.buildHandshakeResult(handshakeResult);
        HAMessage replyMessage = new HAMessage(HAMessageType.MASTER_HANDSHAKE, nettyHAService.getCurrentMasterEpoch(),
            RemotingSerializable.encode(handshakeMaster));
        channel.writeAndFlush(replyMessage);
    }

    public void responseEpochList(Channel channel) {
        List<EpochEntry> entries = nettyHAService.getEpochEntries();
        // Set epoch end offset == message store max offset
        if (entries.size() > 0) {
            entries.get(entries.size() - 1).setEndOffset(nettyHAService.getDefaultMessageStore().getMaxPhyOffset());
        }
        ByteBuffer byteBuffer = ByteBuffer.allocate(3 * 8 * entries.size());
        for (EpochEntry entry : entries) {
            byteBuffer.putLong(entry.getEpoch()).putLong(entry.getStartOffset()).putLong(entry.getEndOffset());
        }
        byteBuffer.flip();
        HAMessage replyMessage = new HAMessage(HAMessageType.RETURN_EPOCH,
            nettyHAService.getCurrentMasterEpoch(), byteBuffer);
        //System.out.println("server send epoch: " + entries + " size: " + entries.size());
        channel.writeAndFlush(replyMessage);
    }

    /**
     * Master change state to transfer and start push data to slave
     */
    public void confirmTruncate(HAMessage message, Channel channel) {
        ConfirmTruncate confirmTruncate = RemotingSerializable.decode(message.getBytes(), ConfirmTruncate.class);
//        System.out.println("master receive truncate offset: " + confirmTruncate.getCommitLogStartOffset());
        nettyHAService.confirmTruncate(channel, confirmTruncate);
    }

    public void pushCommitLogAck(HAMessage message, Channel channel) {
        PushCommitLogAck pushCommitLogAck = RemotingSerializable.decode(message.getBytes(), PushCommitLogAck.class);
//        System.out.println("master receive ack offset: " + pushCommitLogAck.getConfirmOffset()
//            + " count: " + nettyHAService.getConnectionCount());
        nettyHAService.pushCommitLogDataAck(channel, pushCommitLogAck);
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, HAMessage message) throws Exception {
        if (message == null || message.getType() == null) {
            return;
        }

        if (nettyHAService.getCurrentMasterEpoch() != message.getEpoch()) {
//            System.out.printf("server epoch not match, currentMaster=%s, client=%s%n",
//                nettyHAService.getCurrentMasterEpoch(), message.getEpoch());
            log.error("epoch not match, connection epoch:{}", message.getEpoch());
            RemotingUtil.closeChannel(ctx.channel());
            return;
        }

//        if (!message.getType().equals(HAMessageType.PUSH_ACK)) {
//            System.out.println(message.getType());
//        }

        Channel channel = ctx.channel();
        switch (message.getType()) {
            case SLAVE_HANDSHAKE:
                slaveHandshake(message, channel);
                break;
            case QUERY_EPOCH:
                responseEpochList(channel);
                break;
            case CONFIRM_TRUNCATE:
                confirmTruncate(message, channel);
                break;
            case PUSH_ACK:
                pushCommitLogAck(message, channel);
                break;
            default:
                System.out.println("invalid message");
                log.info("receive invalid message, ", message.getType());
        }
    }
}
