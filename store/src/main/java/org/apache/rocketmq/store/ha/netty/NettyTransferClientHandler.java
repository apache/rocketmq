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
import io.netty.channel.ChannelInboundHandlerAdapter;
import java.util.ArrayList;
import java.util.List;
import org.apache.rocketmq.common.EpochEntry;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;
import org.apache.rocketmq.store.ha.autoswitch.AutoSwitchHAClient;
import org.apache.rocketmq.store.ha.protocol.HandshakeMaster;
import org.apache.rocketmq.store.ha.protocol.PushCommitLogData;

public class NettyTransferClientHandler extends ChannelInboundHandlerAdapter {

    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    private final AutoSwitchHAClient autoSwitchHAClient;

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        if (cause != null) {
            log.error("Client channel exception", cause);
        }
        super.exceptionCaught(ctx, cause);
    }

    @Override
    public void channelUnregistered(ChannelHandlerContext ctx) throws Exception {
        autoSwitchHAClient.changePromise(false);
        super.channelUnregistered(ctx);
    }

    public NettyTransferClientHandler(AutoSwitchHAClient autoSwitchHAClient) {
        this.autoSwitchHAClient = autoSwitchHAClient;
    }

    public void masterHandshake(ChannelHandlerContext ctx, TransferMessage request) {
        HandshakeMaster handshakeMaster = RemotingSerializable.decode(request.getBytes(), HandshakeMaster.class);
        autoSwitchHAClient.masterHandshake(handshakeMaster);
    }

    public void returnEpoch(ChannelHandlerContext ctx, TransferMessage request) {
        int remaining = request.getByteBuffer().remaining();
        if (remaining % (3 * 8) != 0) {
            throw new RuntimeException();
        }
        int size = remaining / 24;
        List<EpochEntry> entryList = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            EpochEntry entry = new EpochEntry(request.getByteBuffer().getLong(), request.getByteBuffer().getLong());
            entry.setEndOffset(request.getByteBuffer().getLong());
            entryList.add(entry);
        }
        autoSwitchHAClient.doConsistencyRepairWithMaster(entryList);
    }

    public void pushData(ChannelHandlerContext ctx, TransferMessage message) {
        PushCommitLogData pushCommitLogData = new PushCommitLogData();
        pushCommitLogData.setEpoch(message.getByteBuffer().getLong());
        pushCommitLogData.setEpochStartOffset(message.getByteBuffer().getLong());
        pushCommitLogData.setConfirmOffset(message.getByteBuffer().getLong());
        pushCommitLogData.setBlockStartOffset(message.getByteBuffer().getLong());
        autoSwitchHAClient.doPutCommitLog(pushCommitLogData, message.getByteBuffer());
        autoSwitchHAClient.sendPushCommitLogAck();
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {

        if (!(msg instanceof TransferMessage)) {
            return;
        }

        TransferMessage request = (TransferMessage) msg;

        if (ctx != null) {
            log.debug("Receive request, {} {} {}", request.getType(),
                RemotingHelper.parseChannelRemoteAddr(ctx.channel()), request);
        }

        if (request.getType() == null || autoSwitchHAClient.getCurrentMasterEpoch() != request.getEpoch()) {
            log.error("Epoch not match, connection epoch:{}", request.getEpoch());
            autoSwitchHAClient.closeMaster();
            return;
        } else {
            autoSwitchHAClient.setLastReadTimestamp(System.currentTimeMillis());
        }

        switch (request.getType()) {
            case HANDSHAKE_MASTER:
                this.masterHandshake(ctx, request);
                break;
            case RETURN_EPOCH:
                this.returnEpoch(ctx, request);
                break;
            case TRANSFER_DATA:
                this.pushData(ctx, request);
                break;
            default:
                log.error("receive request type {} not supported", request.getType());
        }
    }
}
