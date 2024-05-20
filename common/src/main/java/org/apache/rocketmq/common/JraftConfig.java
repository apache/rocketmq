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
package org.apache.rocketmq.common;

public class JraftConfig {
    private int jRaftElectionTimeoutMs = 1000;

    private int jRaftScanWaitTimeoutMs = 1000;
    private int jRaftSnapshotIntervalSecs = 3600;
    private String jRaftGroupId = "jRaft-Controller";
    private String jRaftServerId = "localhost:9880";
    private String jRaftInitConf = "localhost:9880,localhost:9881,localhost:9882";
    private String jRaftControllerRPCAddr = "localhost:9770,localhost:9771,localhost:9772";

    public int getjRaftElectionTimeoutMs() {
        return jRaftElectionTimeoutMs;
    }

    public void setjRaftElectionTimeoutMs(int jRaftElectionTimeoutMs) {
        this.jRaftElectionTimeoutMs = jRaftElectionTimeoutMs;
    }

    public int getjRaftSnapshotIntervalSecs() {
        return jRaftSnapshotIntervalSecs;
    }

    public void setjRaftSnapshotIntervalSecs(int jRaftSnapshotIntervalSecs) {
        this.jRaftSnapshotIntervalSecs = jRaftSnapshotIntervalSecs;
    }

    public String getjRaftGroupId() {
        return jRaftGroupId;
    }

    public void setjRaftGroupId(String jRaftGroupId) {
        this.jRaftGroupId = jRaftGroupId;
    }

    public String getjRaftServerId() {
        return jRaftServerId;
    }

    public void setjRaftServerId(String jRaftServerId) {
        this.jRaftServerId = jRaftServerId;
    }

    public String getjRaftInitConf() {
        return jRaftInitConf;
    }

    public void setjRaftInitConf(String jRaftInitConf) {
        this.jRaftInitConf = jRaftInitConf;
    }

    public String getjRaftControllerRPCAddr() {
        return jRaftControllerRPCAddr;
    }

    public void setjRaftControllerRPCAddr(String jRaftControllerRPCAddr) {
        this.jRaftControllerRPCAddr = jRaftControllerRPCAddr;
    }

    public String getjRaftAddress() {
        return this.jRaftServerId;
    }

    public int getjRaftScanWaitTimeoutMs() {
        return jRaftScanWaitTimeoutMs;
    }

    public void setjRaftScanWaitTimeoutMs(int jRaftScanWaitTimeoutMs) {
        this.jRaftScanWaitTimeoutMs = jRaftScanWaitTimeoutMs;
    }
}