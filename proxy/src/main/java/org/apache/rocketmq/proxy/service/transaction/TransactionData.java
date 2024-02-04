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

package org.apache.rocketmq.proxy.service.transaction;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import com.google.common.collect.ComparisonChain;

public class TransactionData implements Comparable<TransactionData> {
    private final String brokerName;
    private final long tranStateTableOffset;
    private final long commitLogOffset;
    private final String transactionId;
    private final long checkTimestamp;
    private final long expireMs;

    public TransactionData(String brokerName, long tranStateTableOffset, long commitLogOffset, String transactionId,
        long checkTimestamp, long expireMs) {
        this.brokerName = brokerName;
        this.tranStateTableOffset = tranStateTableOffset;
        this.commitLogOffset = commitLogOffset;
        this.transactionId = transactionId;
        this.checkTimestamp = checkTimestamp;
        this.expireMs = expireMs;
    }

    public String getBrokerName() {
        return brokerName;
    }

    public long getTranStateTableOffset() {
        return tranStateTableOffset;
    }

    public long getCommitLogOffset() {
        return commitLogOffset;
    }

    public String getTransactionId() {
        return transactionId;
    }

    public long getCheckTimestamp() {
        return checkTimestamp;
    }

    public long getExpireMs() {
        return expireMs;
    }

    public long getExpireTime() {
        return checkTimestamp + expireMs;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TransactionData data = (TransactionData) o;
        return tranStateTableOffset == data.tranStateTableOffset && commitLogOffset == data.commitLogOffset &&
            getExpireTime() == data.getExpireTime() && Objects.equal(brokerName, data.brokerName) &&
            Objects.equal(transactionId, data.transactionId);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(brokerName, transactionId, tranStateTableOffset, commitLogOffset, getExpireTime());
    }

    @Override
    public int compareTo(TransactionData o) {
        return ComparisonChain.start()
            .compare(getExpireTime(), o.getExpireTime())
            .compare(brokerName, o.brokerName)
            .compare(commitLogOffset, o.commitLogOffset)
            .compare(tranStateTableOffset, o.tranStateTableOffset)
            .compare(transactionId, o.transactionId)
            .result();
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("brokerName", brokerName)
            .add("tranStateTableOffset", tranStateTableOffset)
            .add("commitLogOffset", commitLogOffset)
            .add("transactionId", transactionId)
            .add("checkTimestamp", checkTimestamp)
            .add("expireMs", expireMs)
            .toString();
    }
}
