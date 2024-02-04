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
package org.apache.rocketmq.broker.longpolling;

import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.rocketmq.remoting.protocol.RemotingCommand;

import io.netty.channel.Channel;

public class NotificationRequest {
    private RemotingCommand remotingCommand;
    private Channel channel;
    private long expired;
    private AtomicBoolean complete = new AtomicBoolean(false);

    public NotificationRequest(RemotingCommand remotingCommand, Channel channel, long expired) {
        this.channel = channel;
        this.remotingCommand = remotingCommand;
        this.expired = expired;
    }

    public Channel getChannel() {
        return channel;
    }

    public RemotingCommand getRemotingCommand() {
        return remotingCommand;
    }

    public boolean isTimeout() {
        return System.currentTimeMillis() > (expired - 500);
    }

    public boolean complete() {
        return complete.compareAndSet(false, true);
    }

    @Override
    public String toString() {
        return remotingCommand.toString();
    }
}
