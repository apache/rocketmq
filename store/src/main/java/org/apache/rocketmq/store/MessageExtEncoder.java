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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.UnpooledByteBufAllocator;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExtBatch;
import org.apache.rocketmq.common.message.MessageExtBrokerInner;
import org.apache.rocketmq.common.message.MessageVersion;
import org.apache.rocketmq.common.sysflag.MessageSysFlag;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

public class MessageExtEncoder {
    protected static final Logger log = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);
    private ByteBuf byteBuf;
    // The maximum length of the message body.
    private int maxMessageBodySize;
    // The maximum length of the full message.
    private int maxMessageSize;
    public MessageExtEncoder(final int maxMessageBodySize) {
        ByteBufAllocator alloc = UnpooledByteBufAllocator.DEFAULT;
        //Reserve 64kb for encoding buffer outside body
        int maxMessageSize = Integer.MAX_VALUE - maxMessageBodySize >= 64 * 1024 ?
            maxMessageBodySize + 64 * 1024 : Integer.MAX_VALUE;
        byteBuf = alloc.directBuffer(maxMessageSize);
        this.maxMessageBodySize = maxMessageBodySize;
        this.maxMessageSize = maxMessageSize;
    }

    public static int calMsgLength(MessageVersion messageVersion,
        int sysFlag, int bodyLength, int topicLength, int propertiesLength) {

        int bornhostLength = (sysFlag & MessageSysFlag.BORNHOST_V6_FLAG) == 0 ? 8 : 20;
        int storehostAddressLength = (sysFlag & MessageSysFlag.STOREHOSTADDRESS_V6_FLAG) == 0 ? 8 : 20;

        return 4 //TOTALSIZE
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
            + messageVersion.getTopicLengthSize() + topicLength //TOPIC
            + 2 + (Math.max(propertiesLength, 0)); //propertiesLength
    }

    public PutMessageResult encode(MessageExtBrokerInner msgInner) {
        this.byteBuf.clear();
        /**
         * Serialize message
         */
        final byte[] propertiesData =
            msgInner.getPropertiesString() == null ? null : msgInner.getPropertiesString().getBytes(MessageDecoder.CHARSET_UTF8);

        final int propertiesLength = propertiesData == null ? 0 : propertiesData.length;

        if (propertiesLength > Short.MAX_VALUE) {
            log.warn("putMessage message properties length too long. length={}", propertiesData.length);
            return new PutMessageResult(PutMessageStatus.PROPERTIES_SIZE_EXCEEDED, null);
        }

        final byte[] topicData = msgInner.getTopic().getBytes(MessageDecoder.CHARSET_UTF8);
        final int topicLength = topicData.length;

        final int bodyLength = msgInner.getBody() == null ? 0 : msgInner.getBody().length;
        final int msgLen = calMsgLength(
            msgInner.getVersion(), msgInner.getSysFlag(), bodyLength, topicLength, propertiesLength);

        // Exceeds the maximum message body
        if (bodyLength > this.maxMessageBodySize) {
            CommitLog.log.warn("message body size exceeded, msg total size: " + msgLen + ", msg body size: " + bodyLength
                + ", maxMessageSize: " + this.maxMessageBodySize);
            return new PutMessageResult(PutMessageStatus.MESSAGE_ILLEGAL, null);
        }

        final long queueOffset = msgInner.getQueueOffset();

        // Exceeds the maximum message
        if (msgLen > this.maxMessageSize) {
            CommitLog.log.warn("message size exceeded, msg total size: " + msgLen + ", msg body size: " + bodyLength
                + ", maxMessageSize: " + this.maxMessageSize);
            return new PutMessageResult(PutMessageStatus.MESSAGE_ILLEGAL, null);
        }

        // 1 TOTALSIZE
        this.byteBuf.writeInt(msgLen);
        // 2 MAGICCODE
        this.byteBuf.writeInt(msgInner.getVersion().getMagicCode());
        // 3 BODYCRC
        this.byteBuf.writeInt(msgInner.getBodyCRC());
        // 4 QUEUEID
        this.byteBuf.writeInt(msgInner.getQueueId());
        // 5 FLAG
        this.byteBuf.writeInt(msgInner.getFlag());
        // 6 QUEUEOFFSET
        this.byteBuf.writeLong(queueOffset);
        // 7 PHYSICALOFFSET, need update later
        this.byteBuf.writeLong(0);
        // 8 SYSFLAG
        this.byteBuf.writeInt(msgInner.getSysFlag());
        // 9 BORNTIMESTAMP
        this.byteBuf.writeLong(msgInner.getBornTimestamp());

        // 10 BORNHOST
        ByteBuffer bornHostBytes = msgInner.getBornHostBytes();
        this.byteBuf.writeBytes(bornHostBytes.array());

        // 11 STORETIMESTAMP
        this.byteBuf.writeLong(msgInner.getStoreTimestamp());

        // 12 STOREHOSTADDRESS
        ByteBuffer storeHostBytes = msgInner.getStoreHostBytes();
        this.byteBuf.writeBytes(storeHostBytes.array());

        // 13 RECONSUMETIMES
        this.byteBuf.writeInt(msgInner.getReconsumeTimes());
        // 14 Prepared Transaction Offset
        this.byteBuf.writeLong(msgInner.getPreparedTransactionOffset());
        // 15 BODY
        this.byteBuf.writeInt(bodyLength);
        if (bodyLength > 0)
            this.byteBuf.writeBytes(msgInner.getBody());

        // 16 TOPIC
        if (MessageVersion.MESSAGE_VERSION_V2.equals(msgInner.getVersion())) {
            this.byteBuf.writeShort((short) topicLength);
        } else {
            this.byteBuf.writeByte((byte) topicLength);
        }
        this.byteBuf.writeBytes(topicData);

        // 17 PROPERTIES
        this.byteBuf.writeShort((short) propertiesLength);
        if (propertiesLength > 0)
            this.byteBuf.writeBytes(propertiesData);

        return null;
    }

    public ByteBuffer encode(final MessageExtBatch messageExtBatch, PutMessageContext putMessageContext) {
        this.byteBuf.clear();

        ByteBuffer messagesByteBuff = messageExtBatch.wrap();

        int totalLength = messagesByteBuff.limit();
        if (totalLength > this.maxMessageBodySize) {
            CommitLog.log.warn("message body size exceeded, msg body size: " + totalLength + ", maxMessageSize: " + this.maxMessageBodySize);
            throw new RuntimeException("message body size exceeded");
        }

        // properties from MessageExtBatch
        String batchPropStr = MessageDecoder.messageProperties2String(messageExtBatch.getProperties());
        final byte[] batchPropData = batchPropStr.getBytes(MessageDecoder.CHARSET_UTF8);
        int batchPropDataLen = batchPropData.length;
        if (batchPropDataLen > Short.MAX_VALUE) {
            CommitLog.log.warn("Properties size of messageExtBatch exceeded, properties size: {}, maxSize: {}.", batchPropDataLen, Short.MAX_VALUE);
            throw new RuntimeException("Properties size of messageExtBatch exceeded!");
        }
        final short batchPropLen = (short) batchPropDataLen;

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
            boolean needAppendLastPropertySeparator = propertiesLen > 0 && batchPropLen > 0
                && messagesByteBuff.get(messagesByteBuff.position() - 1) != MessageDecoder.PROPERTY_SEPARATOR;

            final byte[] topicData = messageExtBatch.getTopic().getBytes(MessageDecoder.CHARSET_UTF8);

            final int topicLength = topicData.length;
            final int topicLengthSize = messageExtBatch.getVersion().getTopicLengthSize();
            int totalPropLen = needAppendLastPropertySeparator ?
                propertiesLen + batchPropLen + topicLengthSize : propertiesLen + batchPropLen;

            final int msgLen = calMsgLength(
                messageExtBatch.getVersion(), messageExtBatch.getSysFlag(), bodyLen, topicLength, totalPropLen);

            // 1 TOTALSIZE
            this.byteBuf.writeInt(msgLen);
            // 2 MAGICCODE
            this.byteBuf.writeInt(messageExtBatch.getVersion().getMagicCode());
            // 3 BODYCRC
            this.byteBuf.writeInt(bodyCrc);
            // 4 QUEUEID
            this.byteBuf.writeInt(messageExtBatch.getQueueId());
            // 5 FLAG
            this.byteBuf.writeInt(flag);
            // 6 QUEUEOFFSET
            this.byteBuf.writeLong(0);
            // 7 PHYSICALOFFSET
            this.byteBuf.writeLong(0);
            // 8 SYSFLAG
            this.byteBuf.writeInt(messageExtBatch.getSysFlag());
            // 9 BORNTIMESTAMP
            this.byteBuf.writeLong(messageExtBatch.getBornTimestamp());

            // 10 BORNHOST
            ByteBuffer bornHostBytes = messageExtBatch.getBornHostBytes();
            this.byteBuf.writeBytes(bornHostBytes.array());

            // 11 STORETIMESTAMP
            this.byteBuf.writeLong(messageExtBatch.getStoreTimestamp());

            // 12 STOREHOSTADDRESS
            ByteBuffer storeHostBytes = messageExtBatch.getStoreHostBytes();
            this.byteBuf.writeBytes(storeHostBytes.array());

            // 13 RECONSUMETIMES
            this.byteBuf.writeInt(messageExtBatch.getReconsumeTimes());
            // 14 Prepared Transaction Offset, batch does not support transaction
            this.byteBuf.writeLong(0);
            // 15 BODY
            this.byteBuf.writeInt(bodyLen);
            if (bodyLen > 0)
                this.byteBuf.writeBytes(messagesByteBuff.array(), bodyPos, bodyLen);

            // 16 TOPIC
            if (MessageVersion.MESSAGE_VERSION_V2.equals(messageExtBatch.getVersion())) {
                this.byteBuf.writeShort((short) topicLength);
            } else {
                this.byteBuf.writeByte((byte) topicLength);
            }
            this.byteBuf.writeBytes(topicData);

            // 17 PROPERTIES
            this.byteBuf.writeShort((short) totalPropLen);
            if (propertiesLen > 0) {
                this.byteBuf.writeBytes(messagesByteBuff.array(), propertiesPos, propertiesLen);
            }
            if (batchPropLen > 0) {
                if (needAppendLastPropertySeparator) {
                    this.byteBuf.writeByte((byte) MessageDecoder.PROPERTY_SEPARATOR);
                }
                this.byteBuf.writeBytes(batchPropData, 0, batchPropLen);
            }
        }
        putMessageContext.setBatchSize(batchSize);
        putMessageContext.setPhyPos(new long[batchSize]);

        return this.byteBuf.nioBuffer();
    }

    public ByteBuffer getEncoderBuffer() {
        return this.byteBuf.nioBuffer();
    }

    public int getMaxMessageBodySize() {
        return this.maxMessageBodySize;
    }

    public void updateEncoderBufferCapacity(int newMaxMessageBodySize) {
        this.maxMessageBodySize = newMaxMessageBodySize;
        //Reserve 64kb for encoding buffer outside body
        this.maxMessageSize = Integer.MAX_VALUE - newMaxMessageBodySize >= 64 * 1024 ?
            this.maxMessageBodySize + 64 * 1024 : Integer.MAX_VALUE;
        this.byteBuf.capacity(this.maxMessageSize);
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
