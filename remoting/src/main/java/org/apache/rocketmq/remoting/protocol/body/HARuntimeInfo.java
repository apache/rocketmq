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

package org.apache.rocketmq.remoting.protocol.body;

import java.util.ArrayList;
import java.util.List;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;

public class HARuntimeInfo extends RemotingSerializable {

    private boolean master;
    private long masterCommitLogMaxOffset;
    private int inSyncSlaveNums;
    private List<HAConnectionRuntimeInfo> haConnectionInfo = new ArrayList<>();
    private HAClientRuntimeInfo haClientRuntimeInfo = new HAClientRuntimeInfo();

    public boolean isMaster() {
        return this.master;
    }

    public void setMaster(boolean master) {
        this.master = master;
    }

    public long getMasterCommitLogMaxOffset() {
        return this.masterCommitLogMaxOffset;
    }

    public void setMasterCommitLogMaxOffset(long masterCommitLogMaxOffset) {
        this.masterCommitLogMaxOffset = masterCommitLogMaxOffset;
    }

    public int getInSyncSlaveNums() {
        return this.inSyncSlaveNums;
    }

    public void setInSyncSlaveNums(int inSyncSlaveNums) {
        this.inSyncSlaveNums = inSyncSlaveNums;
    }

    public List<HAConnectionRuntimeInfo> getHaConnectionInfo() {
        return this.haConnectionInfo;
    }

    public void setHaConnectionInfo(List<HAConnectionRuntimeInfo> haConnectionInfo) {
        this.haConnectionInfo = haConnectionInfo;
    }

    public HAClientRuntimeInfo getHaClientRuntimeInfo() {
        return this.haClientRuntimeInfo;
    }

    public void setHaClientRuntimeInfo(HAClientRuntimeInfo haClientRuntimeInfo) {
        this.haClientRuntimeInfo = haClientRuntimeInfo;
    }

    public static class HAConnectionRuntimeInfo extends RemotingSerializable {
        private String addr;
        private long slaveAckOffset;
        private long diff;
        private boolean inSync;
        private long transferredByteInSecond;
        private long transferFromWhere;

        public String getAddr() {
            return this.addr;
        }

        public void setAddr(String addr) {
            this.addr = addr;
        }

        public long getSlaveAckOffset() {
            return this.slaveAckOffset;
        }

        public void setSlaveAckOffset(long slaveAckOffset) {
            this.slaveAckOffset = slaveAckOffset;
        }

        public long getDiff() {
            return this.diff;
        }

        public void setDiff(long diff) {
            this.diff = diff;
        }

        public boolean isInSync() {
            return this.inSync;
        }

        public void setInSync(boolean inSync) {
            this.inSync = inSync;
        }

        public long getTransferredByteInSecond() {
            return this.transferredByteInSecond;
        }

        public void setTransferredByteInSecond(long transferredByteInSecond) {
            this.transferredByteInSecond = transferredByteInSecond;
        }

        public long getTransferFromWhere() {
            return transferFromWhere;
        }

        public void setTransferFromWhere(long transferFromWhere) {
            this.transferFromWhere = transferFromWhere;
        }
    }

    public static class HAClientRuntimeInfo extends RemotingSerializable {
        private String masterAddr;
        private long transferredByteInSecond;
        private long maxOffset;
        private long lastReadTimestamp;
        private long lastWriteTimestamp;
        private long masterFlushOffset;
        private boolean isActivated = false;

        public String getMasterAddr() {
            return this.masterAddr;
        }

        public void setMasterAddr(String masterAddr) {
            this.masterAddr = masterAddr;
        }

        public long getTransferredByteInSecond() {
            return this.transferredByteInSecond;
        }

        public void setTransferredByteInSecond(long transferredByteInSecond) {
            this.transferredByteInSecond = transferredByteInSecond;
        }

        public long getMaxOffset() {
            return this.maxOffset;
        }

        public void setMaxOffset(long maxOffset) {
            this.maxOffset = maxOffset;
        }

        public long getLastReadTimestamp() {
            return this.lastReadTimestamp;
        }

        public void setLastReadTimestamp(long lastReadTimestamp) {
            this.lastReadTimestamp = lastReadTimestamp;
        }

        public long getLastWriteTimestamp() {
            return this.lastWriteTimestamp;
        }

        public void setLastWriteTimestamp(long lastWriteTimestamp) {
            this.lastWriteTimestamp = lastWriteTimestamp;
        }

        public long getMasterFlushOffset() {
            return masterFlushOffset;
        }

        public void setMasterFlushOffset(long masterFlushOffset) {
            this.masterFlushOffset = masterFlushOffset;
        }
    }

}
