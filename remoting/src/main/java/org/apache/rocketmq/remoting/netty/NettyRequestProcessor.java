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

import io.netty.channel.ChannelHandlerContext;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

/**
 * <p>
 *     This interface defines a contract for request processor.
 * </p>
 *
 * <p>
 *     <strong>Thread Safety:</strong> implementation of this interface MUST be thread-safe as the processor is normally
 *     executed concurrently.
 * </p>
 */
public interface NettyRequestProcessor {

    /**
     * In the high level, this method would process the incoming <code>request</code> then generate a response
     * accordingly.
     *
     * @param ctx The channel handler context.
     * @param request Incoming request.
     * @return Response command.
     * @throws Exception if there is any error.
     */
    RemotingCommand processRequest(ChannelHandlerContext ctx, RemotingCommand request) throws Exception;

    /**
     * Check if current request should be rejected.
     * @return true if reject; false otherwise.
     */
    boolean rejectRequest();
}
