/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.rocketmq.proxy.service.relay;

import java.util.concurrent.CompletableFuture;

public class RelayData<T, R> {
    private T processResult;
    private CompletableFuture<ProxyRelayResult<R>> relayFuture;

    public RelayData(T processResult, CompletableFuture<ProxyRelayResult<R>> relayFuture) {
        this.processResult = processResult;
        this.relayFuture = relayFuture;
    }

    public CompletableFuture<ProxyRelayResult<R>> getRelayFuture() {
        return relayFuture;
    }

    public void setRelayFuture(
        CompletableFuture<ProxyRelayResult<R>> relayFuture) {
        this.relayFuture = relayFuture;
    }

    public T getProcessResult() {
        return processResult;
    }

    public void setProcessResult(T processResult) {
        this.processResult = processResult;
    }
}
