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
package org.apache.rocketmq.store;

import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExtBatch;
import org.apache.rocketmq.common.message.MessageExtBrokerInner;
import org.apache.rocketmq.common.sysflag.MessageSysFlag;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;

public class MessageExtEncoder {
    protected static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);
    // Store the message content
    private final ByteBuffer encoderBuffer;
    // The maximum length of the message
    private final int maxMessageSize;

    MessageExtEncoder(final int size) {
        this.encoderBuffer = ByteBuffer.allocateDirect(size);
        this.maxMessageSize = size;
    }

    public static int calMsgLength(int sysFlag, int bodyLength, int topicLength, int propertiesLength) {
        int bornhostLength = (sysFlag & MessageSysFlag.BORNHOST_V6_FLAG) == 0 ? 8 : 20;
        int storehostAddressLength = (sysFlag & MessageSysFlag.STOREHOSTADDRESS_V6_FLAG) == 0 ? 8 : 20;
        final int msgLen = 4 //TOTALSIZE
            + 4 //MAGICCODE
            + 4 //BODYCRC
            + 4 //QUEUEID
            + 4 //FLAG
            + 8 //QUEUEOFFSET
            + 8 //PHYSICALOFFSET
            + 4 //SYSFLAG
            + 8 //BORNTIMESTAMP
            + bornhostLength //BORNHOST
            + 8 //STORETIMESTAMP
            + storehostAddressLength //STOREHOSTADDRESS
            + 4 //RECONSUMETIMES
            + 8 //Prepared Transaction Offset
            + 4 + (Math.max(bodyLength, 0)) //BODY
            + 1 + topicLength //TOPIC
            + 2 + (Math.max(propertiesLength, 0)); //propertiesLength
        return msgLen;
    }

    private void socketAddress2ByteBuffer(final SocketAddress socketAddress, final ByteBuffer byteBuffer) {
        InetSocketAddress inetSocketAddress = (InetSocketAddress) socketAddress;
        InetAddress address = inetSocketAddress.getAddress();
        if (address instanceof Inet4Address) {
            byteBuffer.put(inetSocketAddress.getAddress().getAddress(), 0, 4);
        } else {
            byteBuffer.put(inetSocketAddress.getAddress().getAddress(), 0, 16);
        }
        byteBuffer.putInt(inetSocketAddress.getPort());
    }

    protected PutMessageResult encode(MessageExtBrokerInner msgInner) {
        /**
         * Serialize message
         */
        final byte[] propertiesData =
            msgInner.getPropertiesString() == null ? null : msgInner.getPropertiesString().getBytes(
                MessageDecoder.CHARSET_UTF8);

        final int propertiesLength = propertiesData == null ? 0 : propertiesData.length;

        if (propertiesLength > Short.MAX_VALUE) {
            log.warn("putMessage message properties length too long. length={}", propertiesData.length);
            return new PutMessageResult(PutMessageStatus.PROPERTIES_SIZE_EXCEEDED, null);
        }

        final byte[] topicData = msgInner.getTopic().getBytes(MessageDecoder.CHARSET_UTF8);
        final int topicLength = topicData.length;

        final int bodyLength = msgInner.getBody() == null ? 0 : msgInner.getBody().length;

        final int msgLen = calMsgLength(msgInner.getSysFlag(), bodyLength, topicLength, propertiesLength);

        final long queueOffset = msgInner.getQueueOffset();

        // Exceeds the maximum message
        if (msgLen > this.maxMessageSize) {
            CommitLog.log.warn("message size exceeded, msg total size: " + msgLen + ", msg body size: " + bodyLength
                + ", maxMessageSize: " + this.maxMessageSize);
            return new PutMessageResult(PutMessageStatus.MESSAGE_ILLEGAL, null);
        }

        // Initialization of storage space
        this.resetByteBuffer(encoderBuffer, msgLen);
        // 1 TOTALSIZE
        this.encoderBuffer.putInt(msgLen);
        // 2 MAGICCODE
        this.encoderBuffer.putInt(CommitLog.MESSAGE_MAGIC_CODE);
        // 3 BODYCRC
        this.encoderBuffer.putInt(msgInner.getBodyCRC());
        // 4 QUEUEID
        this.encoderBuffer.putInt(msgInner.getQueueId());
        // 5 FLAG
        this.encoderBuffer.putInt(msgInner.getFlag());
        // 6 QUEUEOFFSET
        this.encoderBuffer.putLong(queueOffset);
        // 7 PHYSICALOFFSET, need update later
        this.encoderBuffer.putLong(0);
        // 8 SYSFLAG
        this.encoderBuffer.putInt(msgInner.getSysFlag());
        // 9 BORNTIMESTAMP
        this.encoderBuffer.putLong(msgInner.getBornTimestamp());
        // 10 BORNHOST
        socketAddress2ByteBuffer(msgInner.getBornHost() ,this.encoderBuffer);
        // 11 STORETIMESTAMP
        this.encoderBuffer.putLong(msgInner.getStoreTimestamp());
        // 12 STOREHOSTADDRESS
        socketAddress2ByteBuffer(msgInner.getStoreHost() ,this.encoderBuffer);
        // 13 RECONSUMETIMES
        this.encoderBuffer.putInt(msgInner.getReconsumeTimes());
        // 14 Prepared Transaction Offset
        this.encoderBuffer.putLong(msgInner.getPreparedTransactionOffset());
        // 15 BODY
        this.encoderBuffer.putInt(bodyLength);
        if (bodyLength > 0)
            this.encoderBuffer.put(msgInner.getBody());
        // 16 TOPIC
        this.encoderBuffer.put((byte) topicLength);
        this.encoderBuffer.put(topicData);
        // 17 PROPERTIES
        this.encoderBuffer.putShort((short) propertiesLength);
        if (propertiesLength > 0)
            this.encoderBuffer.put(propertiesData);

        encoderBuffer.flip();
        return null;
    }

    protected ByteBuffer encode(final MessageExtBatch messageExtBatch, PutMessageContext putMessageContext) {
        encoderBuffer.clear(); //not thread-safe
        int totalMsgLen = 0;
        ByteBuffer messagesByteBuff = messageExtBatch.wrap();

        int sysFlag = messageExtBatch.getSysFlag();
        int bornHostLength = (sysFlag & MessageSysFlag.BORNHOST_V6_FLAG) == 0 ? 4 + 4 : 16 + 4;
        int storeHostLength = (sysFlag & MessageSysFlag.STOREHOSTADDRESS_V6_FLAG) == 0 ? 4 + 4 : 16 + 4;
        ByteBuffer bornHostHolder = ByteBuffer.allocate(bornHostLength);
        ByteBuffer storeHostHolder = ByteBuffer.allocate(storeHostLength);

        // properties from MessageExtBatch
        String batchPropStr = MessageDecoder.messageProperties2String(messageExtBatch.getProperties());
        final byte[] batchPropData = batchPropStr.getBytes(MessageDecoder.CHARSET_UTF8);
        final short batchPropLen = (short) batchPropData.length;

        int batchSize = 0;
        while (messagesByteBuff.hasRemaining()) {
            batchSize++;
            // 1 TOTALSIZE
            messagesByteBuff.getInt();
            // 2 MAGICCODE
            messagesByteBuff.getInt();
            // 3 BODYCRC
            messagesByteBuff.getInt();
            // 4 FLAG
            int flag = messagesByteBuff.getInt();
            // 5 BODY
            int bodyLen = messagesByteBuff.getInt();
            int bodyPos = messagesByteBuff.position();
            int bodyCrc = UtilAll.crc32(messagesByteBuff.array(), bodyPos, bodyLen);
            messagesByteBuff.position(bodyPos + bodyLen);
            // 6 properties
            short propertiesLen = messagesByteBuff.getShort();
            int propertiesPos = messagesByteBuff.position();
            messagesByteBuff.position(propertiesPos + propertiesLen);

            final byte[] topicData = messageExtBatch.getTopic().getBytes(MessageDecoder.CHARSET_UTF8);

            final int topicLength = topicData.length;

            final int msgLen = calMsgLength(messageExtBatch.getSysFlag(), bodyLen, topicLength,
                propertiesLen + batchPropLen);

            // Exceeds the maximum message
            if (msgLen > this.maxMessageSize) {
                CommitLog.log.warn("message size exceeded, msg total size: " + msgLen + ", msg body size: " + bodyLen
                    + ", maxMessageSize: " + this.maxMessageSize);
                throw new RuntimeException("message size exceeded");
            }

            totalMsgLen += msgLen;
            // Determines whether there is sufficient free space
            if (totalMsgLen > maxMessageSize) {
                throw new RuntimeException("message size exceeded");
            }

            // 1 TOTALSIZE
            this.encoderBuffer.putInt(msgLen);
            // 2 MAGICCODE
            this.encoderBuffer.putInt(CommitLog.MESSAGE_MAGIC_CODE);
            // 3 BODYCRC
            this.encoderBuffer.putInt(bodyCrc);
            // 4 QUEUEID
            this.encoderBuffer.putInt(messageExtBatch.getQueueId());
            // 5 FLAG
            this.encoderBuffer.putInt(flag);
            // 6 QUEUEOFFSET
            this.encoderBuffer.putLong(0);
            // 7 PHYSICALOFFSET
            this.encoderBuffer.putLong(0);
            // 8 SYSFLAG
            this.encoderBuffer.putInt(messageExtBatch.getSysFlag());
            // 9 BORNTIMESTAMP
            this.encoderBuffer.putLong(messageExtBatch.getBornTimestamp());
            // 10 BORNHOST
            this.resetByteBuffer(bornHostHolder, bornHostLength);
            this.encoderBuffer.put(messageExtBatch.getBornHostBytes(bornHostHolder));
            // 11 STORETIMESTAMP
            this.encoderBuffer.putLong(messageExtBatch.getStoreTimestamp());
            // 12 STOREHOSTADDRESS
            this.resetByteBuffer(storeHostHolder, storeHostLength);
            this.encoderBuffer.put(messageExtBatch.getStoreHostBytes(storeHostHolder));
            // 13 RECONSUMETIMES
            this.encoderBuffer.putInt(messageExtBatch.getReconsumeTimes());
            // 14 Prepared Transaction Offset, batch does not support transaction
            this.encoderBuffer.putLong(0);
            // 15 BODY
            this.encoderBuffer.putInt(bodyLen);
            if (bodyLen > 0)
                this.encoderBuffer.put(messagesByteBuff.array(), bodyPos, bodyLen);
            // 16 TOPIC
            this.encoderBuffer.put((byte) topicLength);
            this.encoderBuffer.put(topicData);
            // 17 PROPERTIES
            this.encoderBuffer.putShort((short) (propertiesLen + batchPropLen));
            if (propertiesLen > 0) {
                this.encoderBuffer.put(messagesByteBuff.array(), propertiesPos, propertiesLen);
            }
            if (batchPropLen > 0) {
                this.encoderBuffer.put(batchPropData, 0, batchPropLen);
            }
        }
        putMessageContext.setBatchSize(batchSize);
        putMessageContext.setPhyPos(new long[batchSize]);
        encoderBuffer.flip();
        return encoderBuffer;
    }

    public ByteBuffer getEncoderBuffer() {
        return encoderBuffer;
    }

    private void resetByteBuffer(final ByteBuffer byteBuffer, final int limit) {
        byteBuffer.flip();
        byteBuffer.limit(limit);
    }

    static class PutMessageThreadLocal {
        private final MessageExtEncoder encoder;
        private final StringBuilder keyBuilder;
        PutMessageThreadLocal(int size) {
            encoder = new MessageExtEncoder(size);
            keyBuilder = new StringBuilder();
        }

        public MessageExtEncoder getEncoder() {
            return encoder;
        }

        public StringBuilder getKeyBuilder() {
            return keyBuilder;
        }
    }

}
