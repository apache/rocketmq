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

package org.apache.rocketmq.proxy.service.channel;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

public class InvocationContext implements InvocationContextInterface {
    private final CompletableFuture<RemotingCommand> response;
    private final long timestamp = System.currentTimeMillis();

    public InvocationContext(CompletableFuture<RemotingCommand> resp) {
        this.response = resp;
    }

    public boolean expired(long expiredTimeSec) {
        return System.currentTimeMillis() - timestamp >= Duration.ofSeconds(expiredTimeSec).toMillis();
    }

    public CompletableFuture<RemotingCommand> getResponse() {
        return response;
    }

    public void handle(RemotingCommand remotingCommand) {
        response.complete(remotingCommand);
    }
}
