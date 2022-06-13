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

import com.google.common.base.Preconditions;
import java.util.concurrent.CompletableFuture;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.RemotingSysResponseCode;

/**
 * Provide a handler adaptor over RpcHook.
 */
public class HandlerAdaptor implements Handler {

    private final RPCHook rpcHook;

    public HandlerAdaptor(RPCHook rpcHook) {
        Preconditions.checkNotNull(rpcHook, "RpcHook should not null");
        this.rpcHook = rpcHook;
    }

    @Override
    public Decision preHandle(HandlerContext context, RemotingCommand request,
        CompletableFuture<RemotingCommand> responseFuture) {
        try {
            rpcHook.doBeforeRequest(context.peerAddress(), request);
        } catch (Throwable e) {
            RemotingCommand response = RemotingCommand.createResponseCommand(RemotingSysResponseCode.SYSTEM_ERROR, e.getMessage());
            response.setOpaque(request.getOpaque());
            responseFuture.complete(response);
            return Decision.STOP;
        }
        return Decision.CONTINUE;
    }

    @Override public Decision postHandle(HandlerContext context, RemotingCommand request,
        RemotingCommand response) {
        try {
            rpcHook.doAfterResponse(context.peerAddress(), request, response);
        } catch (Throwable e) {
            return Decision.STOP;
        }
        return Decision.CONTINUE;
    }
}
