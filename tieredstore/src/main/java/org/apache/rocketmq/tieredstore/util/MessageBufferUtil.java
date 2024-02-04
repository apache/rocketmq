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
package org.apache.rocketmq.tieredstore.util;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.tieredstore.file.TieredCommitLog;
import org.apache.rocketmq.tieredstore.file.TieredConsumeQueue;

public class MessageBufferUtil {
    private static final Logger logger = LoggerFactory.getLogger(TieredStoreUtil.TIERED_STORE_LOGGER_NAME);

    public static final int QUEUE_OFFSET_POSITION = 4 /* total size */
        + 4 /* magic code */
        + 4 /* body CRC */
        + 4 /* queue id */
        + 4; /* flag */

    public static final int PHYSICAL_OFFSET_POSITION = 4 /* total size */
        + 4 /* magic code */
        + 4 /* body CRC */
        + 4 /* queue id */
        + 4 /* flag */
        + 8; /* queue offset */

    public static final int SYS_FLAG_OFFSET_POSITION = 4 /* total size */
        + 4 /* magic code */
        + 4 /* body CRC */
        + 4 /* queue id */
        + 4 /* flag */
        + 8 /* queue offset */
        + 8; /* physical offset */

    public static final int STORE_TIMESTAMP_POSITION = 4 /* total size */
        + 4 /* magic code */
        + 4 /* body CRC */
        + 4 /* queue id */
        + 4 /* flag */
        + 8 /* queue offset */
        + 8 /* physical offset */
        + 4 /* sys flag */
        + 8 /* born timestamp */
        + 8; /* born host */

    public static final int STORE_HOST_POSITION = 4 /* total size */
        + 4 /* magic code */
        + 4 /* body CRC */
        + 4 /* queue id */
        + 4 /* flag */
        + 8 /* queue offset */
        + 8 /* physical offset */
        + 4 /* sys flag */
        + 8 /* born timestamp */
        + 8 /* born host */
        + 8; /* store timestamp */

    public static int getTotalSize(ByteBuffer message) {
        return message.getInt(message.position());
    }

    public static int getMagicCode(ByteBuffer message) {
        return message.getInt(message.position() + 4);
    }

    public static long getQueueOffset(ByteBuffer message) {
        return message.getLong(message.position() + QUEUE_OFFSET_POSITION);
    }

    public static long getCommitLogOffset(ByteBuffer message) {
        return message.getLong(message.position() + PHYSICAL_OFFSET_POSITION);
    }

    public static long getStoreTimeStamp(ByteBuffer message) {
        return message.getLong(message.position() + STORE_TIMESTAMP_POSITION);
    }

    public static ByteBuffer getOffsetIdBuffer(ByteBuffer message) {
        ByteBuffer idBuffer = ByteBuffer.allocate(TieredStoreUtil.MSG_ID_LENGTH);
        idBuffer.limit(TieredStoreUtil.MSG_ID_LENGTH);
        idBuffer.putLong(message.getLong(message.position() + STORE_HOST_POSITION));
        idBuffer.putLong(getCommitLogOffset(message));
        idBuffer.flip();
        return idBuffer;
    }

    public static String getOffsetId(ByteBuffer message) {
        return UtilAll.bytes2string(getOffsetIdBuffer(message).array());
    }

    public static Map<String, String> getProperties(ByteBuffer message) {
        ByteBuffer slice = message.slice();
        return MessageDecoder.decodeProperties(slice);
    }

    public static List<Pair<Integer/* offset of msgBuffer */, Integer/* msg size */>> splitMessageBuffer(
        ByteBuffer cqBuffer, ByteBuffer msgBuffer) {
        cqBuffer.rewind();
        msgBuffer.rewind();
        List<Pair<Integer, Integer>> messageList = new ArrayList<>(cqBuffer.remaining() / TieredConsumeQueue.CONSUME_QUEUE_STORE_UNIT_SIZE);
        if (cqBuffer.remaining() % TieredConsumeQueue.CONSUME_QUEUE_STORE_UNIT_SIZE != 0) {
            logger.warn("MessageBufferUtil#splitMessage: consume queue buffer size {} is not an integer multiple of CONSUME_QUEUE_STORE_UNIT_SIZE {}",
                cqBuffer.remaining(), TieredConsumeQueue.CONSUME_QUEUE_STORE_UNIT_SIZE);
            return messageList;
        }
        try {
            long startCommitLogOffset = CQItemBufferUtil.getCommitLogOffset(cqBuffer);
            for (int pos = cqBuffer.position(); pos < cqBuffer.limit(); pos += TieredConsumeQueue.CONSUME_QUEUE_STORE_UNIT_SIZE) {
                cqBuffer.position(pos);
                int diff = (int) (CQItemBufferUtil.getCommitLogOffset(cqBuffer) - startCommitLogOffset);
                int size = CQItemBufferUtil.getSize(cqBuffer);
                if (diff + size > msgBuffer.limit()) {
                    logger.error("MessageBufferUtil#splitMessage: message buffer size is incorrect: record in consume queue: {}, actual: {}", diff + size, msgBuffer.remaining());
                    return messageList;
                }
                msgBuffer.position(diff);

                int magicCode = getMagicCode(msgBuffer);
                if (magicCode == TieredCommitLog.BLANK_MAGIC_CODE) {
                    logger.warn("MessageBufferUtil#splitMessage: message decode error: blank magic code, this message may be coda, try to fix offset");
                    diff = diff + TieredCommitLog.CODA_SIZE;
                    msgBuffer.position(diff);
                    magicCode = getMagicCode(msgBuffer);
                }
                if (magicCode != MessageDecoder.MESSAGE_MAGIC_CODE && magicCode != MessageDecoder.MESSAGE_MAGIC_CODE_V2) {
                    logger.warn("MessageBufferUtil#splitMessage: message decode error: unknown magic code");
                    continue;
                }

                if (getTotalSize(msgBuffer) != size) {
                    logger.warn("MessageBufferUtil#splitMessage: message size is not right: except: {}, actual: {}", size, getTotalSize(msgBuffer));
                    continue;
                }

                messageList.add(Pair.of(diff, size));
            }
        } catch (Exception e) {
            logger.error("MessageBufferUtil#splitMessage: split message failed, maybe decode consume queue item failed", e);
        } finally {
            cqBuffer.rewind();
            msgBuffer.rewind();
        }
        return messageList;
    }
}
