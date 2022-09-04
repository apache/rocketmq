/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.rocketmq.store.ha.netty;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.util.AttributeKey;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.common.RemotingUtil;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;
import org.apache.rocketmq.store.ha.autoswitch.AutoSwitchHAConnection;
import org.apache.rocketmq.store.ha.autoswitch.AutoSwitchHAService;
import org.apache.rocketmq.store.ha.protocol.ConfirmTruncate;
import org.apache.rocketmq.store.ha.protocol.HandshakeMaster;
import org.apache.rocketmq.store.ha.protocol.HandshakeResult;
import org.apache.rocketmq.store.ha.protocol.HandshakeSlave;
import org.apache.rocketmq.store.ha.protocol.PushCommitLogAck;

public class NettyTransferServerHandler extends SimpleChannelInboundHandler<TransferMessage> {

    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    private final AutoSwitchHAService autoSwitchHAService;
    private AutoSwitchHAConnection haConnection;

    public NettyTransferServerHandler(AutoSwitchHAService autoSwitchHAService) {
        this.autoSwitchHAService = autoSwitchHAService;
    }

    @Override
    public void channelUnregistered(ChannelHandlerContext ctx) throws Exception {
        if (haConnection != null) {
            haConnection.shutdown();
        }
        super.channelUnregistered(ctx);
    }

    public void slaveHandshake(ChannelHandlerContext ctx, TransferMessage message) {
        HandshakeSlave handshakeSlave = RemotingSerializable.decode(message.getBytes(), HandshakeSlave.class);
        HandshakeResult handshakeResult = autoSwitchHAService.verifySlaveIdentity(handshakeSlave);
        HandshakeMaster handshakeMaster = autoSwitchHAService.buildHandshakeResult(handshakeResult);

        this.haConnection = new AutoSwitchHAConnection(autoSwitchHAService, ctx.channel());
        this.haConnection.setSlaveBrokerId(handshakeSlave.getBrokerId());
        this.haConnection.setClientAddress(handshakeSlave.getBrokerAddr());
        ctx.channel().attr(AttributeKey.valueOf("connection")).set(this.haConnection);
        this.autoSwitchHAService.getNettyTransferServer().addChannelToGroup(ctx.channel());

        TransferMessage response = autoSwitchHAService.buildMessage(TransferType.HANDSHAKE_MASTER);
        response.appendBody(RemotingSerializable.encode(handshakeMaster));
        ctx.channel().writeAndFlush(response);
    }

    public void responseEpochList(ChannelHandlerContext ctx, TransferMessage message) {
        this.haConnection.sendEpochEntries();
    }

    /**
     * Master change state to transfer and start push data to slave
     */
    public void confirmTruncate(ChannelHandlerContext ctx, TransferMessage message) {
        ConfirmTruncate confirmTruncate = RemotingSerializable.decode(message.getBytes(), ConfirmTruncate.class);
        haConnection.confirmTruncate(confirmTruncate);
    }

    public void pushCommitLogAck(ChannelHandlerContext ctx, TransferMessage message) {
        PushCommitLogAck pushCommitLogAck = RemotingSerializable.decode(message.getBytes(), PushCommitLogAck.class);
        haConnection.pushCommitLogDataAck(pushCommitLogAck);
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, TransferMessage request) {

        if (ctx != null) {
            log.debug("Receive request, {} {} {}", request != null ? request.getType() : "unknown",
                RemotingHelper.parseChannelRemoteAddr(ctx.channel()), request);
        } else {
            return;
        }

        if (request == null || request.getType() == null) {
            log.error("Receive empty request");
            return;
        }

        if (!autoSwitchHAService.isSlaveEpochMatchMaster(request.getEpoch())) {
            log.info("Receive empty request, epoch not match, connection epoch:{}", request.getEpoch());
            RemotingUtil.closeChannel(ctx.channel());
            return;
        }

        System.out.println(request.getType());
        switch (request.getType()) {
            case HANDSHAKE_SLAVE:
                this.slaveHandshake(ctx, request);
                break;
            case QUERY_EPOCH:
                this.responseEpochList(ctx, request);
                break;
            case CONFIRM_TRUNCATE:
                this.confirmTruncate(ctx, request);
                break;
            case TRANSFER_ACK:
                this.pushCommitLogAck(ctx, request);
                break;
            default:
                log.error("Receive request type {} not supported", request.getType());
        }
    }
}
