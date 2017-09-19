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

package org.apache.rocketmq.rpc.impl.context;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.apache.rocketmq.remoting.api.command.RemotingCommand;

public class RpcCallerContext {
    private RemotingCommand remotingRequest;
    private RemotingCommand remotingResponse;

    public RemotingCommand getRemotingRequest() {
        return remotingRequest;
    }

    public void setRemotingRequest(RemotingCommand remotingRequest) {
        this.remotingRequest = remotingRequest;
    }

    public RemotingCommand getRemotingResponse() {
        return remotingResponse;
    }

    public void setRemotingResponse(RemotingCommand remotingResponse) {
        this.remotingResponse = remotingResponse;
    }

    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this, ToStringStyle.MULTI_LINE_STYLE);
    }
}
