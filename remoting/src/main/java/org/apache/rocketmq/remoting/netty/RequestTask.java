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

import io.netty.channel.Channel;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

public class RequestTask implements Runnable {
    private final Runnable runnable;
    private final long createTimestamp = System.currentTimeMillis();
    private final Channel channel;
    private final RemotingCommand request;

    private static final int INIT = 0;
    private static final int RUNNING = 1;
    private static final int DONE = 2;
    private static final int CANCEL = -1;
    private AtomicInteger status = new AtomicInteger(INIT);

    public RequestTask(final Runnable runnable, final Channel channel, final RemotingCommand request) {
        this.runnable = runnable;
        this.channel = channel;
        this.request = request;
    }

    @Override
    public int hashCode() {
        int result = runnable != null ? runnable.hashCode() : 0;
        result = 31 * result + (int) (getCreateTimestamp() ^ (getCreateTimestamp() >>> 32));
        result = 31 * result + (channel != null ? channel.hashCode() : 0);
        result = 31 * result + (request != null ? request.hashCode() : 0);
        result = 31 * result + (status.get());
        return result;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o)
            return true;
        if (!(o instanceof RequestTask))
            return false;

        final RequestTask that = (RequestTask) o;

        if (getCreateTimestamp() != that.getCreateTimestamp())
            return false;
        if (status.get() != that.status.get())
            return false;
        if (channel != null ? !channel.equals(that.channel) : that.channel != null)
            return false;
        return request != null ? request.getOpaque() == that.request.getOpaque() : that.request == null;

    }

    public long getCreateTimestamp() {
        return createTimestamp;
    }

    public boolean cancel() {
        return this.status.compareAndSet(INIT, CANCEL);
    }

    public boolean isCancelled() {
        return this.status.get() == CANCEL;
    }

    @Override
    public void run() {
        if (this.status.compareAndSet(INIT, RUNNING)) {
            this.runnable.run();
            this.status.set(DONE);
        }
    }

    public void returnResponse(int code, String remark) {
        final RemotingCommand response = RemotingCommand.createResponseCommand(code, remark);
        response.setOpaque(request.getOpaque());
        this.channel.writeAndFlush(response);
    }
}
