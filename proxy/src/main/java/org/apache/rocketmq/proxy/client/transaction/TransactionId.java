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
package org.apache.rocketmq.proxy.client.transaction;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
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
    private String gatewayTransactionId;

    public TransactionId(SocketAddress brokerAddr, String brokerTransactionId, long commitLogOffset,
        long tranStateTableOffset, String gatewayTransactionId) {
        this.brokerAddr = brokerAddr;
        this.brokerTransactionId = brokerTransactionId;
        this.commitLogOffset = commitLogOffset;
        this.tranStateTableOffset = tranStateTableOffset;
        this.gatewayTransactionId = gatewayTransactionId;
    }

    public TransactionId() {
    }

    public static TransactionId genFromBrokerTransactionId(String brokerAddr, SendResult sendResult) {
        MessageId id = new MessageId(null, 0);
        try {
            if (sendResult.getOffsetMsgId() != null) {
                id = MessageDecoder.decodeMessageId(sendResult.getOffsetMsgId());
            } else {
                id = MessageDecoder.decodeMessageId(sendResult.getMsgId());
            }
        } catch (Exception e) {
            log.warn("genFromBrokerTransactionId failed. brokerAddr: {}, sendResult: {}", brokerAddr, sendResult, e);
        }
        return genFromBrokerTransactionId(RemotingUtil.string2SocketAddress(brokerAddr), sendResult.getTransactionId(),
            id.getOffset(), sendResult.getQueueOffset());
    }

    public static TransactionId genFromBrokerTransactionId(SocketAddress brokerAddr, String orgTransactionId,
        long commitLogOffset, long tranStateTableOffset) {
        byte[] orgTransactionIdByte = orgTransactionId.getBytes(StandardCharsets.UTF_8);

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
            .gatewayTransactionId(gatewayTransactionId)
            .build();
    }

    public static TransactionId genFromGatewayTransactionId(String gatewayTransactionId) throws UnknownHostException {
        ByteBuffer byteBuffer = ByteBuffer.wrap(UtilAll.string2bytes(gatewayTransactionId));

        byte[] ip = new byte[4];
        byteBuffer.get(ip);
        int port = byteBuffer.getInt();
        SocketAddress brokerAddr = new InetSocketAddress(InetAddress.getByAddress(ip), port);

        int orgTransactionIdLen = byteBuffer.getInt();
        byte[] orgTransactionIdByte = new byte[orgTransactionIdLen];
        byteBuffer.get(orgTransactionIdByte);

        long commitLogOffset = byteBuffer.getLong();
        long tranStateTableOffset = byteBuffer.getLong();

        return TransactionId.builder()
            .brokerAddr(brokerAddr)
            .brokerTransactionId(new String(orgTransactionIdByte, StandardCharsets.UTF_8))
            .commitLogOffset(commitLogOffset)
            .tranStateTableOffset(tranStateTableOffset)
            .gatewayTransactionId(gatewayTransactionId)
            .build();
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
        return commitLogOffset == id.commitLogOffset && tranStateTableOffset == id.tranStateTableOffset && Objects.equals(brokerAddr, id.brokerAddr) && Objects.equals(brokerTransactionId, id.brokerTransactionId) && Objects.equals(gatewayTransactionId, id.gatewayTransactionId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(brokerAddr, brokerTransactionId, commitLogOffset, tranStateTableOffset, gatewayTransactionId);
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

    public String getGatewayTransactionId() {
        return this.gatewayTransactionId;
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

    public void setGatewayTransactionId(String gatewayTransactionId) {
        this.gatewayTransactionId = gatewayTransactionId;
    }

    public String toString() {
        return "TransactionId(brokerAddr=" + this.getBrokerAddr() + ", brokerTransactionId=" + this.getBrokerTransactionId() + ", commitLogOffset=" + this.getCommitLogOffset() + ", tranStateTableOffset=" + this.getTranStateTableOffset() + ", gatewayTransactionId=" + this.getGatewayTransactionId() + ")";
    }

    public static class TransactionIdBuilder {
        private SocketAddress brokerAddr;
        private String brokerTransactionId;
        private long commitLogOffset;
        private long tranStateTableOffset;
        private String gatewayTransactionId;

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

        public TransactionIdBuilder gatewayTransactionId(String gatewayTransactionId) {
            this.gatewayTransactionId = gatewayTransactionId;
            return this;
        }

        public TransactionId build() {
            return new TransactionId(brokerAddr, brokerTransactionId, commitLogOffset, tranStateTableOffset, gatewayTransactionId);
        }

        public String toString() {
            return "TransactionId.TransactionIdBuilder(brokerAddr=" + this.brokerAddr + ", brokerTransactionId=" + this.brokerTransactionId + ", commitLogOffset=" + this.commitLogOffset + ", tranStateTableOffset=" + this.tranStateTableOffset + ", gatewayTransactionId=" + this.gatewayTransactionId + ")";
        }
    }
}
