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
package org.apache.rocketmq.acl.plain;

import org.apache.rocketmq.common.PlainAccessConfig;

import java.util.ArrayList;
import java.util.List;

public class PlainAccessData {
    List<String> globalWhiteRemoteAddresses = new ArrayList<>();
    List<PlainAccessConfig> accounts = new ArrayList<>();
    List<DataVersion> dataVersion = new ArrayList<>();

    public List<String> getGlobalWhiteRemoteAddresses() {
        return globalWhiteRemoteAddresses;
    }

    public void setGlobalWhiteRemoteAddresses(List<String> globalWhiteRemoteAddresses) {
        this.globalWhiteRemoteAddresses = globalWhiteRemoteAddresses;
    }

    public List<PlainAccessConfig> getAccounts() {
        return accounts;
    }

    public void setAccounts(List<PlainAccessConfig> accounts) {
        this.accounts = accounts;
    }

    public List<DataVersion> getDataVersion() {
        return dataVersion;
    }

    public void setDataVersion(List<DataVersion> dataVersion) {
        this.dataVersion = dataVersion;
    }

    public static class DataVersion {
        private long timestamp;
        private long counter;

        public long getTimestamp() {
            return timestamp;
        }

        public void setTimestamp(long timestamp) {
            this.timestamp = timestamp;
        }

        public long getCounter() {
            return counter;
        }

        public void setCounter(long counter) {
            this.counter = counter;
        }
    }
}
