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
package org.apache.rocketmq.store.timer;

import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.store.logfile.MappedFile;
import org.apache.rocketmq.store.MappedFileQueue;
import org.apache.rocketmq.store.SelectMappedBufferResult;

import java.nio.ByteBuffer;

public class TimerLog {
    private static Logger log = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);
    public final static int BLANK_MAGIC_CODE = 0xBBCCDDEE ^ 1880681586 + 8;
    private final static int MIN_BLANK_LEN = 4 + 8 + 4;
    public final static int UNIT_SIZE = 4  //size
            + 8 //prev pos
            + 4 //magic value
            + 8 //curr write time, for trace
            + 4 //delayed time, for check
            + 8 //offsetPy
            + 4 //sizePy
            + 4 //hash code of real topic
            + 8; //reserved value, just in case of
    public final static int UNIT_PRE_SIZE_FOR_MSG = 28;
    public final static int UNIT_PRE_SIZE_FOR_METRIC = 40;
    private final MappedFileQueue mappedFileQueue;

    private final int fileSize;

    public TimerLog(final String storePath, final int fileSize) {
        this.fileSize = fileSize;
        this.mappedFileQueue = new MappedFileQueue(storePath, fileSize, null);
    }

    public boolean load() {
        return this.mappedFileQueue.load();
    }

    public long append(byte[] data) {
        return append(data, 0, data.length);
    }

    public long append(byte[] data, int pos, int len) {
        MappedFile mappedFile = this.mappedFileQueue.getLastMappedFile();
        if (null == mappedFile || mappedFile.isFull()) {
            mappedFile = this.mappedFileQueue.getLastMappedFile(0);
        }
        if (null == mappedFile) {
            log.error("Create mapped file1 error for timer log");
            return -1;
        }
        if (len + MIN_BLANK_LEN > mappedFile.getFileSize() - mappedFile.getWrotePosition()) {
            ByteBuffer byteBuffer = ByteBuffer.allocate(MIN_BLANK_LEN);
            byteBuffer.putInt(mappedFile.getFileSize() - mappedFile.getWrotePosition());
            byteBuffer.putLong(0);
            byteBuffer.putInt(BLANK_MAGIC_CODE);
            if (mappedFile.appendMessage(byteBuffer.array())) {
                //need to set the wrote position
                mappedFile.setWrotePosition(mappedFile.getFileSize());
            } else {
                log.error("Append blank error for timer log");
                return -1;
            }
            mappedFile = this.mappedFileQueue.getLastMappedFile(0);
            if (null == mappedFile) {
                log.error("create mapped file2 error for timer log");
                return -1;
            }
        }
        long currPosition = mappedFile.getFileFromOffset() + mappedFile.getWrotePosition();
        if (!mappedFile.appendMessage(data, pos, len)) {
            log.error("Append error for timer log");
            return -1;
        }
        return currPosition;
    }

    public SelectMappedBufferResult getTimerMessage(long offsetPy) {
        MappedFile mappedFile = mappedFileQueue.findMappedFileByOffset(offsetPy);
        if (null == mappedFile)
            return null;
        return mappedFile.selectMappedBuffer((int) (offsetPy % mappedFile.getFileSize()));
    }

    public SelectMappedBufferResult getWholeBuffer(long offsetPy) {
        MappedFile mappedFile = mappedFileQueue.findMappedFileByOffset(offsetPy);
        if (null == mappedFile)
            return null;
        return mappedFile.selectMappedBuffer(0);
    }

    public MappedFileQueue getMappedFileQueue() {
        return mappedFileQueue;
    }

    public void shutdown() {
        this.mappedFileQueue.flush(0);
        //it seems do not need to call shutdown
    }

    // be careful.
    // if the format of timerlog changed, this offset has to be changed too
    // so dose the batch writing
    public int getOffsetForLastUnit() {

        return fileSize - (fileSize - MIN_BLANK_LEN) % UNIT_SIZE - MIN_BLANK_LEN - UNIT_SIZE;
    }

}
