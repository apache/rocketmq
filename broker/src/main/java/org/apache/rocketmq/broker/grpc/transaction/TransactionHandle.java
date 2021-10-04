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

package org.apache.rocketmq.broker.grpc.transaction;

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import java.util.List;

public class TransactionHandle {
    private final static String SEPARATOR = "%";
    private final String messageId;
    private final long transactionStateTableOffset;
    private final long commitLogOffset;

    public static TransactionHandle decode(String transactionId) {
        List<String> decodeList = Lists.newArrayList(Splitter.on(" ").split(transactionId));
        if (decodeList.size() < 3) {
            throw new IllegalArgumentException("Invalid argument transactionId");
        }
        String messageId = decodeList.get(0);
        long transactionStateTableOffset = Long.parseLong(decodeList.get(1));
        long commitLogOffset = Long.parseLong(decodeList.get(2));
        return new TransactionHandle(messageId, transactionStateTableOffset, commitLogOffset);
    }

    public String encode() {
        return messageId + SEPARATOR + transactionStateTableOffset + SEPARATOR + commitLogOffset;
    }

    public TransactionHandle(String messageId, long transactionStateTableOffset, long commitLogOffset) {
        this.messageId = messageId;
        this.transactionStateTableOffset = transactionStateTableOffset;
        this.commitLogOffset = commitLogOffset;
    }

    public String getMessageId() {
        return messageId;
    }

    public long getTransactionStateTableOffset() {
        return transactionStateTableOffset;
    }

    public long getCommitLogOffset() {
        return commitLogOffset;
    }
}
