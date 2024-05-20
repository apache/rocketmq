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

package org.apache.rocketmq.remoting.pipeline;

import io.netty.channel.ChannelHandlerContext;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

public interface RequestPipeline {

    void execute(ChannelHandlerContext ctx, RemotingCommand request) throws Exception;

    default void executeResponse(ChannelHandlerContext ctx, RemotingCommand request, RemotingCommand response) throws Exception {
    }

    default RequestPipeline pipe(RequestPipeline source) {
        if (source == null) {
            return this;
        }
        return new RequestPipeline() {
            @Override
            public void execute(ChannelHandlerContext ctx, RemotingCommand request) throws Exception {
                source.execute(ctx, request);
                RequestPipeline.this.execute(ctx, request);
            }

            @Override
            public void executeResponse(ChannelHandlerContext ctx, RemotingCommand request, RemotingCommand response) throws Exception {
                RequestPipeline.this.executeResponse(ctx, request, response);
                source.executeResponse(ctx, request, response);
            }
        };
    }
}
