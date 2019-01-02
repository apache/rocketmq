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

package org.apache.rocketmq.remoting;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

public interface RemotingChannel {
    /**
     * Returns the local address where this {@code RemotingChannel} is bound to.  The returned {@link SocketAddress} is
     * supposed to be down-cast into more concrete type such as {@link InetSocketAddress} to retrieve the detailed
     * information.
     *
     * @return the local address of this channel. {@code null} if this channel is not bound.
     */
    SocketAddress localAddress();

    /**
     * Returns the remote address where this {@code RemotingChannel} is connected to.  The returned {@link
     * SocketAddress} is supposed to be down-cast into more concrete type such as {@link InetSocketAddress} to retrieve
     * the detailed information.
     *
     * @return the remote address of this channel. {@code null} if this channel is not connected.
     */
    SocketAddress remoteAddress();

    /**
     * Returns {@code true} if and only if the I/O thread will perform the requested write operation immediately.  Any
     * write requests made when this method returns {@code false} are queued until the I/O thread is ready to process
     * the queued write requests.
     */
    boolean isWritable();

    /**
     * Returns {@code true} if the {@code RemotingChannel} is active and so connected.
     */
    boolean isActive();

    /**
     * Requests to close the {@code RemotingChannel} immediately.
     */
    void close();

    /**
     * Writes a response {@code RemotingCommand} to remote.
     *
     * @param command the response command
     */
    void reply(RemotingCommand command);

}
