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
package org.apache.rocketmq.remoting.netty;

import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.LongAdder;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

@ChannelHandler.Sharable
public class RemotingCodeDistributionHandler extends ChannelDuplexHandler {

    private final ConcurrentMap<Integer, TrafficStats> inboundStats;
    private final ConcurrentMap<Integer, TrafficStats> outboundStats;
    private final NettyServerConfig nettyServerConfig;

    public RemotingCodeDistributionHandler(NettyServerConfig nettyServerConfig) {
        this.inboundStats = new ConcurrentHashMap<>();
        this.outboundStats = new ConcurrentHashMap<>();
        this.nettyServerConfig = nettyServerConfig;
    }

    void recordInbound(RemotingCommand cmd) {
        TrafficStats stats = inboundStats.computeIfAbsent(cmd.getCode(), k -> new TrafficStats());
        stats.count.increment();
        stats.trafficSize.add(calcCommandSize(cmd));
    }

    void recordOutbound(RemotingCommand cmd) {
        TrafficStats stats = outboundStats.computeIfAbsent(cmd.getCode(), k -> new TrafficStats());
        stats.count.increment();
        stats.trafficSize.add(calcCommandSize(cmd));
    }

    /**
     * Protocol fixed overhead in bytes:
     * <pre>
     * frameHeader:  totalLen(4) + headerLenMark(4) = 8
     * fixedHeader:  code(2) + language(1) + version(2) + opaque(4) + flag(4)
     *            + remarkLenPrefix(4) + extFieldsLenPrefix(4) = 21
     * </pre>
     */
    static final int FIXED_OVERHEAD = 4 + 4 + 2 + 1 + 2 + 4 + 4 + 4 + 4;

    private int calcCommandSize(RemotingCommand cmd) {
        int size = FIXED_OVERHEAD;
        byte[] body = cmd.getBody();
        if (body != null) {
            size += body.length;
        }
        if (nettyServerConfig.isEnableDetailedTrafficSize()) {
            size += calcHeaderVariableSize(cmd);
        }
        return size;
    }

    private int calcHeaderVariableSize(RemotingCommand cmd) {
        int size = 0;
        String remark = cmd.getRemark();
        if (remark != null) {
            size += remark.length();
        }
        HashMap<String, String> extFields = cmd.getExtFields();
        if (extFields != null) {
            for (Map.Entry<String, String> entry : extFields.entrySet()) {
                if (entry.getKey() != null && entry.getValue() != null) {
                    size += 2 + entry.getKey().length() + 4 + entry.getValue().length();
                }
            }
        }
        return size;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        if (msg instanceof RemotingCommand) {
            recordInbound((RemotingCommand) msg);
        }
        ctx.fireChannelRead(msg);
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
        if (msg instanceof RemotingCommand) {
            recordOutbound((RemotingCommand) msg);
        }
        ctx.write(msg, promise);
    }

    private Map<Integer, Long> getCountSnapshot(ConcurrentMap<Integer, TrafficStats> statsMap) {
        Map<Integer, Long> map = new HashMap<>(statsMap.size());
        for (Map.Entry<Integer, TrafficStats> entry : statsMap.entrySet()) {
            map.put(entry.getKey(), entry.getValue().count.sumThenReset());
        }
        return map;
    }

    private Map<Integer, Long> getTrafficSnapshot(ConcurrentMap<Integer, TrafficStats> statsMap) {
        Map<Integer, Long> map = new HashMap<>(statsMap.size());
        for (Map.Entry<Integer, TrafficStats> entry : statsMap.entrySet()) {
            map.put(entry.getKey(), entry.getValue().trafficSize.sumThenReset());
        }
        return map;
    }

    private String snapshotToString(Map<Integer, Long> distribution) {
        if (null != distribution && !distribution.isEmpty()) {
            StringBuilder sb = new StringBuilder("{");
            boolean first = true;
            for (Map.Entry<Integer, Long> entry : distribution.entrySet()) {
                if (0L == entry.getValue()) {
                    continue;
                }
                sb.append(first ? "" : ", ").append(entry.getKey()).append(":").append(entry.getValue());
                first = false;
            }
            if (first) {
                return null;
            }
            sb.append("}");
            return sb.toString();
        }
        return null;
    }

    public String getInBoundSnapshotString() {
        return this.snapshotToString(this.getCountSnapshot(this.inboundStats));
    }

    public String getOutBoundSnapshotString() {
        return this.snapshotToString(this.getCountSnapshot(this.outboundStats));
    }

    public String getInBoundTrafficSnapshotString() {
        return this.snapshotToString(this.getTrafficSnapshot(this.inboundStats));
    }

    public String getOutBoundTrafficSnapshotString() {
        return this.snapshotToString(this.getTrafficSnapshot(this.outboundStats));
    }

    static class TrafficStats {
        final LongAdder count = new LongAdder();
        final LongAdder trafficSize = new LongAdder();
    }
}
