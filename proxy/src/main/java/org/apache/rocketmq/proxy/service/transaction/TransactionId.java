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
package org.apache.rocketmq.proxy.service.transaction;

import com.google.common.base.MoreObjects;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageId;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;

public class TransactionId {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.PROXY_LOGGER_NAME);

    private String brokerName;
    private String brokerTransactionId;
    private long commitLogOffset;
    private long tranStateTableOffset;
    private String proxyTransactionId;

    public TransactionId(
        String brokerName,
        String brokerTransactionId,
        long commitLogOffset,
        long tranStateTableOffset,
        String proxyTransactionId
    ) {
        this.brokerName = brokerName;
        this.brokerTransactionId = brokerTransactionId;
        this.commitLogOffset = commitLogOffset;
        this.tranStateTableOffset = tranStateTableOffset;
        this.proxyTransactionId = proxyTransactionId;
    }

    public TransactionId() {
    }

    public static TransactionId genByBrokerTransactionId(String brokerName, SendResult sendResult) {
        long commitLogOffset = 0L;
        try {
            if (sendResult.getOffsetMsgId() != null) {
                commitLogOffset = generateCommitLogOffset(sendResult.getOffsetMsgId());
            } else {
                commitLogOffset = generateCommitLogOffset(sendResult.getMsgId());
            }
        } catch (Exception e) {
            log.warn("genFromBrokerTransactionId failed. brokerName: {}, sendResult: {}", brokerName, sendResult, e);
        }
        return genByBrokerTransactionId(brokerName, sendResult.getTransactionId(),
            commitLogOffset, sendResult.getQueueOffset());
    }

    public static TransactionId genByBrokerTransactionId(
        String brokerName,
        String orgTransactionId,
        long commitLogOffset,
        long tranStateTableOffset
    ) {
        byte[] orgTransactionIdByte = new byte[0];
        if (StringUtils.isNotBlank(orgTransactionId)) {
            orgTransactionIdByte = orgTransactionId.getBytes(StandardCharsets.UTF_8);
        }
        byte[] brokerNameByte = brokerName.getBytes(StandardCharsets.UTF_8);

        ByteBuffer byteBuffer = ByteBuffer.allocate(4 + brokerNameByte.length + 4 + orgTransactionIdByte.length + 8 + 8);

        byteBuffer.putInt(brokerNameByte.length);
        byteBuffer.put(brokerNameByte);
        byteBuffer.putInt(orgTransactionIdByte.length);
        byteBuffer.put(orgTransactionIdByte);
        byteBuffer.putLong(commitLogOffset);
        byteBuffer.putLong(tranStateTableOffset);

        String gatewayTransactionId = UtilAll.bytes2string(byteBuffer.array());

        return TransactionId.builder()
            .brokerName(brokerName)
            .brokerTransactionId(orgTransactionId)
            .commitLogOffset(commitLogOffset)
            .tranStateTableOffset(tranStateTableOffset)
            .proxyTransactionId(gatewayTransactionId)
            .build();
    }

    public static TransactionId decode(String transactionId) throws UnknownHostException {
        ByteBuffer byteBuffer = ByteBuffer.wrap(UtilAll.string2bytes(transactionId));

        int brokerNameLen = byteBuffer.getInt();
        byte[] brokerNameByte = new byte[brokerNameLen];
        byteBuffer.get(brokerNameByte);

        int orgTransactionIdLen = byteBuffer.getInt();
        byte[] orgTransactionIdByte = new byte[0];
        if (orgTransactionIdLen > 0) {
            orgTransactionIdByte = new byte[orgTransactionIdLen];
            byteBuffer.get(orgTransactionIdByte);
        }

        long commitLogOffset = byteBuffer.getLong();
        long tranStateTableOffset = byteBuffer.getLong();

        return TransactionId.builder()
            .brokerName(new String(brokerNameByte, StandardCharsets.UTF_8))
            .brokerTransactionId(new String(orgTransactionIdByte, StandardCharsets.UTF_8))
            .commitLogOffset(commitLogOffset)
            .tranStateTableOffset(tranStateTableOffset)
            .proxyTransactionId(transactionId)
            .build();
    }

    public static long generateCommitLogOffset(String messageId) throws IllegalArgumentException {
        try {
            MessageId id = MessageDecoder.decodeMessageId(messageId);
            return id.getOffset();
        } catch (UnknownHostException e) {
            throw new IllegalArgumentException("illegal messageId: " + messageId);
        }
    }

    public static TransactionIdBuilder builder() {
        return new TransactionIdBuilder();
    }

    public String getBrokerName() {
        return brokerName;
    }

    public void setBrokerName(String brokerName) {
        this.brokerName = brokerName;
    }

    public String getBrokerTransactionId() {
        return brokerTransactionId;
    }

    public void setBrokerTransactionId(String brokerTransactionId) {
        this.brokerTransactionId = brokerTransactionId;
    }

    public long getCommitLogOffset() {
        return commitLogOffset;
    }

    public void setCommitLogOffset(long commitLogOffset) {
        this.commitLogOffset = commitLogOffset;
    }

    public long getTranStateTableOffset() {
        return tranStateTableOffset;
    }

    public void setTranStateTableOffset(long tranStateTableOffset) {
        this.tranStateTableOffset = tranStateTableOffset;
    }

    public String getProxyTransactionId() {
        return proxyTransactionId;
    }

    public void setProxyTransactionId(String proxyTransactionId) {
        this.proxyTransactionId = proxyTransactionId;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("brokerName", brokerName)
            .add("brokerTransactionId", brokerTransactionId)
            .add("commitLogOffset", commitLogOffset)
            .add("tranStateTableOffset", tranStateTableOffset)
            .add("proxyTransactionId", proxyTransactionId)
            .toString();
    }

    public static class TransactionIdBuilder {
        private String brokerName;
        private String brokerTransactionId;
        private long commitLogOffset;
        private long tranStateTableOffset;
        private String proxyTransactionId;

        TransactionIdBuilder() {
        }

        public TransactionIdBuilder brokerName(String brokerName) {
            this.brokerName = brokerName;
            return this;
        }

        public TransactionIdBuilder brokerTransactionId(String brokerTransactionId) {
            this.brokerTransactionId = brokerTransactionId;
            return this;
        }

        public TransactionIdBuilder commitLogOffset(long commitLogOffset) {
            this.commitLogOffset = commitLogOffset;
            return this;
        }

        public TransactionIdBuilder tranStateTableOffset(long tranStateTableOffset) {
            this.tranStateTableOffset = tranStateTableOffset;
            return this;
        }

        public TransactionIdBuilder proxyTransactionId(String proxyTransactionId) {
            this.proxyTransactionId = proxyTransactionId;
            return this;
        }

        public TransactionId build() {
            return new TransactionId(brokerName, brokerTransactionId, commitLogOffset, tranStateTableOffset, proxyTransactionId);
        }

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(this)
                .add("brokerName", brokerName)
                .add("brokerTransactionId", brokerTransactionId)
                .add("commitLogOffset", commitLogOffset)
                .add("tranStateTableOffset", tranStateTableOffset)
                .add("proxyTransactionId", proxyTransactionId)
                .toString();
        }
    }
}
