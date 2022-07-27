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

import org.apache.rocketmq.remoting.protocol.RemotingCommand;

public class AbstractRpcHook implements RPCHook {

    private RPCHookContext handlerContext;

    @Override public void doBeforeRequest(String remoteAddr, RemotingCommand request) {

    }

    @Override public void doAfterResponse(String remoteAddr, RemotingCommand request,
        RemotingCommand response) {

    }

    //This method should move to the parent interface RPCHook in the future
    //Currently, to be compatible with client of jdk 1.6, put it here
    public RPCHookContext getContext() {
        return handlerContext;
    }

    //This method should move to the parent interface RPCHook in the future
    //Currently, to be compatible with client of jdk 1.6, put it here
    public void setContext(RPCHookContext handlerContext) {
        this.handlerContext = handlerContext;
    }
}