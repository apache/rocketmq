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
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.tieredstore.common.SelectBufferResult;
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

    public static List<SelectBufferResult> splitMessageBuffer(ByteBuffer cqBuffer, ByteBuffer msgBuffer) {

        cqBuffer.rewind();
        msgBuffer.rewind();

        List<SelectBufferResult> bufferResultList = new ArrayList<>(
            cqBuffer.remaining() / TieredConsumeQueue.CONSUME_QUEUE_STORE_UNIT_SIZE);

        if (msgBuffer.remaining() == 0) {
            logger.error("MessageBufferUtil#splitMessage, msg buffer length is zero");
            return bufferResultList;
        }

        if (cqBuffer.remaining() % TieredConsumeQueue.CONSUME_QUEUE_STORE_UNIT_SIZE != 0) {
            logger.error("MessageBufferUtil#splitMessage, consume queue buffer size incorrect, {}", cqBuffer.remaining());
            return bufferResultList;
        }

        try {
            long firstCommitLogOffset = CQItemBufferUtil.getCommitLogOffset(cqBuffer);

            for (int position = cqBuffer.position(); position < cqBuffer.limit();
                position += TieredConsumeQueue.CONSUME_QUEUE_STORE_UNIT_SIZE) {

                cqBuffer.position(position);
                long logOffset = CQItemBufferUtil.getCommitLogOffset(cqBuffer);
                int bufferSize = CQItemBufferUtil.getSize(cqBuffer);
                long tagCode = CQItemBufferUtil.getTagCode(cqBuffer);

                int offset = (int) (logOffset - firstCommitLogOffset);
                if (offset + bufferSize > msgBuffer.limit()) {
                    logger.error("MessageBufferUtil#splitMessage, message buffer size incorrect. " +
                        "Expect length in consume queue: {}, actual length: {}", offset + bufferSize, msgBuffer.limit());
                    break;
                }

                msgBuffer.position(offset);
                int magicCode = getMagicCode(msgBuffer);
                if (magicCode == TieredCommitLog.BLANK_MAGIC_CODE) {
                    offset += TieredCommitLog.CODA_SIZE;
                    msgBuffer.position(offset);
                    magicCode = getMagicCode(msgBuffer);
                }
                if (magicCode != MessageDecoder.MESSAGE_MAGIC_CODE &&
                    magicCode != MessageDecoder.MESSAGE_MAGIC_CODE_V2) {
                    logger.warn("MessageBufferUtil#splitMessage, found unknown magic code. " +
                        "Message offset: {}, wrong magic code: {}", offset, magicCode);
                    continue;
                }

                if (bufferSize != getTotalSize(msgBuffer)) {
                    logger.warn("MessageBufferUtil#splitMessage, message length in commitlog incorrect. " +
                        "Except length in commitlog: {}, actual: {}", getTotalSize(msgBuffer), bufferSize);
                    continue;
                }

                ByteBuffer sliceBuffer = msgBuffer.slice();
                sliceBuffer.limit(bufferSize);
                bufferResultList.add(new SelectBufferResult(sliceBuffer, offset, bufferSize, tagCode));
            }
        } catch (Exception e) {
            logger.error("MessageBufferUtil#splitMessage, split message buffer error", e);
        } finally {
            cqBuffer.rewind();
            msgBuffer.rewind();
        }
        return bufferResultList;
    }
}
