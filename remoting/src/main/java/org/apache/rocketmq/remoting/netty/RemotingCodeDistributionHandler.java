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

    private final ConcurrentMap<Integer, LongAdder> inboundDistribution;
    private final ConcurrentMap<Integer, LongAdder> outboundDistribution;

    public RemotingCodeDistributionHandler() {
        inboundDistribution = new ConcurrentHashMap<>();
        outboundDistribution = new ConcurrentHashMap<>();
    }

    private void countInbound(int requestCode) {
        LongAdder item = inboundDistribution.computeIfAbsent(requestCode, k -> new LongAdder());
        item.increment();
    }

    private void countOutbound(int responseCode) {
        LongAdder item = outboundDistribution.computeIfAbsent(responseCode, k -> new LongAdder());
        item.increment();
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        if (msg instanceof RemotingCommand) {
            RemotingCommand cmd = (RemotingCommand) msg;
            countInbound(cmd.getCode());
        }
        ctx.fireChannelRead(msg);
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
        if (msg instanceof RemotingCommand) {
            RemotingCommand cmd = (RemotingCommand) msg;
            countOutbound(cmd.getCode());
        }
        ctx.write(msg, promise);
    }

    private Map<Integer, Long> getDistributionSnapshot(Map<Integer, LongAdder> countMap) {
        Map<Integer, Long> map = new HashMap<>(countMap.size());
        for (Map.Entry<Integer, LongAdder> entry : countMap.entrySet()) {
            map.put(entry.getKey(), entry.getValue().sumThenReset());
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
        return this.snapshotToString(this.getDistributionSnapshot(this.inboundDistribution));
    }

    public String getOutBoundSnapshotString() {
        return this.snapshotToString(this.getDistributionSnapshot(this.outboundDistribution));
    }
}
