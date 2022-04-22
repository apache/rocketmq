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
package org.apache.rocketmq.proxy.connector.transaction;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageId;
import org.apache.rocketmq.remoting.common.RemotingUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TransactionId {
    private static final Logger log = LoggerFactory.getLogger(TransactionId.class);

    private SocketAddress brokerAddr;
    private String brokerTransactionId;
    private long commitLogOffset;
    private long tranStateTableOffset;
    private String proxyTransactionId;

    public TransactionId(
        SocketAddress brokerAddr,
        String brokerTransactionId,
        long commitLogOffset,
        long tranStateTableOffset,
        String proxyTransactionId
    ) {
        this.brokerAddr = brokerAddr;
        this.brokerTransactionId = brokerTransactionId;
        this.commitLogOffset = commitLogOffset;
        this.tranStateTableOffset = tranStateTableOffset;
        this.proxyTransactionId = proxyTransactionId;
    }

    public TransactionId() {
    }

    public static TransactionId genByBrokerTransactionId(String brokerAddr, SendResult sendResult) {
        long commitLogOffset = 0L;
        try {
            if (sendResult.getOffsetMsgId() != null) {
                commitLogOffset = generateCommitLogOffset(sendResult.getOffsetMsgId());
            } else {
                commitLogOffset = generateCommitLogOffset(sendResult.getMsgId());
            }
        } catch (Exception e) {
            log.warn("genFromBrokerTransactionId failed. brokerAddr: {}, sendResult: {}", brokerAddr, sendResult, e);
        }
        return genByBrokerTransactionId(RemotingUtil.string2SocketAddress(brokerAddr), sendResult.getTransactionId(),
            commitLogOffset, sendResult.getQueueOffset());
    }

    public static TransactionId genByBrokerTransactionId(
        SocketAddress brokerAddr,
        String orgTransactionId,
        long commitLogOffset,
        long tranStateTableOffset
    ) {
        byte[] orgTransactionIdByte = new byte[0];
        if (StringUtils.isNotBlank(orgTransactionId)) {
            orgTransactionIdByte = orgTransactionId.getBytes(StandardCharsets.UTF_8);
        }

        ByteBuffer byteBuffer = ByteBuffer.allocate(8 + 4 + orgTransactionIdByte.length + 8 + 8);
        byteBuffer.put(MessageExt.socketAddress2ByteBuffer(brokerAddr));

        byteBuffer.putInt(orgTransactionIdByte.length);
        byteBuffer.put(orgTransactionIdByte);
        byteBuffer.putLong(commitLogOffset);
        byteBuffer.putLong(tranStateTableOffset);

        String gatewayTransactionId = UtilAll.bytes2string(byteBuffer.array());

        return TransactionId.builder()
            .brokerAddr(brokerAddr)
            .brokerTransactionId(orgTransactionId)
            .commitLogOffset(commitLogOffset)
            .tranStateTableOffset(tranStateTableOffset)
            .proxyTransactionId(gatewayTransactionId)
            .build();
    }

    public static TransactionId decode(String transactionId) throws UnknownHostException {
        ByteBuffer byteBuffer = ByteBuffer.wrap(UtilAll.string2bytes(transactionId));

        byte[] ip = new byte[4];
        byteBuffer.get(ip);
        int port = byteBuffer.getInt();
        SocketAddress brokerAddr = new InetSocketAddress(InetAddress.getByAddress(ip), port);

        int orgTransactionIdLen = byteBuffer.getInt();
        byte[] orgTransactionIdByte = new byte[0];
        if (orgTransactionIdLen > 0) {
            orgTransactionIdByte = new byte[orgTransactionIdLen];
            byteBuffer.get(orgTransactionIdByte);
        }

        long commitLogOffset = byteBuffer.getLong();
        long tranStateTableOffset = byteBuffer.getLong();

        return TransactionId.builder()
            .brokerAddr(brokerAddr)
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

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TransactionId id = (TransactionId) o;
        return commitLogOffset == id.commitLogOffset && tranStateTableOffset == id.tranStateTableOffset && Objects.equals(brokerAddr, id.brokerAddr) && Objects.equals(brokerTransactionId, id.brokerTransactionId) && Objects.equals(proxyTransactionId, id.proxyTransactionId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(brokerAddr, brokerTransactionId, commitLogOffset, tranStateTableOffset, proxyTransactionId);
    }

    public static TransactionIdBuilder builder() {
        return new TransactionIdBuilder();
    }

    public SocketAddress getBrokerAddr() {
        return this.brokerAddr;
    }

    public String getBrokerTransactionId() {
        return this.brokerTransactionId;
    }

    public long getCommitLogOffset() {
        return this.commitLogOffset;
    }

    public long getTranStateTableOffset() {
        return this.tranStateTableOffset;
    }

    public String getProxyTransactionId() {
        return this.proxyTransactionId;
    }

    public void setBrokerAddr(SocketAddress brokerAddr) {
        this.brokerAddr = brokerAddr;
    }

    public void setBrokerTransactionId(String brokerTransactionId) {
        this.brokerTransactionId = brokerTransactionId;
    }

    public void setCommitLogOffset(long commitLogOffset) {
        this.commitLogOffset = commitLogOffset;
    }

    public void setTranStateTableOffset(long tranStateTableOffset) {
        this.tranStateTableOffset = tranStateTableOffset;
    }

    public void setProxyTransactionId(String proxyTransactionId) {
        this.proxyTransactionId = proxyTransactionId;
    }

    @Override
    public String toString() {
        return "TransactionId{" +
            "brokerAddr=" + brokerAddr +
            ", brokerTransactionId='" + brokerTransactionId + '\'' +
            ", commitLogOffset=" + commitLogOffset +
            ", tranStateTableOffset=" + tranStateTableOffset +
            ", gatewayTransactionId='" + proxyTransactionId + '\'' +
            '}';
    }

    public static class TransactionIdBuilder {
        private SocketAddress brokerAddr;
        private String brokerTransactionId;
        private long commitLogOffset;
        private long tranStateTableOffset;
        private String proxyTransactionId;

        TransactionIdBuilder() {
        }

        public TransactionIdBuilder brokerAddr(SocketAddress brokerAddr) {
            this.brokerAddr = brokerAddr;
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
            return new TransactionId(brokerAddr, brokerTransactionId, commitLogOffset, tranStateTableOffset, proxyTransactionId);
        }

        @Override
        public String toString() {
            return "TransactionId.TransactionIdBuilder{" +
                "brokerAddr=" + brokerAddr +
                ", brokerTransactionId='" + brokerTransactionId + '\'' +
                ", commitLogOffset=" + commitLogOffset +
                ", tranStateTableOffset=" + tranStateTableOffset +
                ", gatewayTransactionId='" + proxyTransactionId + '\'' +
                '}';
        }
    }
}
