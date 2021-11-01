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

package org.apache.rocketmq.grpc.server;

public class GrpcServerConfig {
    private int port = 10922;
    private int terminationAwaitTimeout = 30;
    private int executorCoreThreadNumber = Runtime.getRuntime().availableProcessors() * 8 + 16;
    private int executorMaxThreadNumber = (Runtime.getRuntime().availableProcessors() + 1) * 16;
    private int executorQueueLength = 10000;
    private long rpcRoadReserveTimeMs = 10;
    private long expiredChannelTimeSec = 120;

    public GrpcServerConfig() {
    }

    public int getPort() {
        return this.port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public int getTerminationAwaitTimeout() {
        return this.terminationAwaitTimeout;
    }

    public void setTerminationAwaitTimeout(int terminationAwaitTimeout) {
        this.terminationAwaitTimeout = terminationAwaitTimeout;
    }

    public int getExecutorCoreThreadNumber() {
        return this.executorCoreThreadNumber;
    }

    public void setExecutorCoreThreadNumber(int executorCoreThreadNumber) {
        this.executorCoreThreadNumber = executorCoreThreadNumber;
    }

    public int getExecutorMaxThreadNumber() {
        return this.executorMaxThreadNumber;
    }

    public void setExecutorMaxThreadNumber(int executorMaxThreadNumber) {
        this.executorMaxThreadNumber = executorMaxThreadNumber;
    }

    public int getExecutorQueueLength() {
        return this.executorQueueLength;
    }

    public void setExecutorQueueLength(int executorQueueLength) {
        this.executorQueueLength = executorQueueLength;
    }

    public long getRpcRoadReserveTimeMs() {
        return rpcRoadReserveTimeMs;
    }

    public void setRpcRoadReserveTimeMs(long rpcRoadReserveTimeMs) {
        this.rpcRoadReserveTimeMs = rpcRoadReserveTimeMs;
    }

    public long getExpiredChannelTimeSec() {
        return expiredChannelTimeSec;
    }

    public void setExpiredChannelTimeSec(long expiredChannelTimeSec) {
        this.expiredChannelTimeSec = expiredChannelTimeSec;
    }
}
