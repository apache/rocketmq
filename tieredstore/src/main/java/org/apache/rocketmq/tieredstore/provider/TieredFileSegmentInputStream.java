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

package org.apache.rocketmq.tieredstore.provider;

import org.apache.rocketmq.tieredstore.util.MessageBufferUtil;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.List;
public class TieredFileSegmentInputStream extends InputStream {

    private final TieredFileSegment.FileSegmentType fileType;
    protected final List<ByteBuffer> uploadBufferList;
    private final int contentLength;

    /**
     * readPosition is the now position in the stream
     */
    protected int readPosition = 0;

    /**
     * curReadBufferIndex is the index of the buffer in uploadBufferList which is being read
     */
    protected int curReadBufferIndex = 0;
    /**
     * readPosInCurBuffer is the position in the buffer which is being read
     */
    protected int readPosInCurBuffer = 0;

    /**
     * curBuffer is the buffer which is being read, it is the same as uploadBufferList.get(curReadBufferIndex)
     */
    protected ByteBuffer curBuffer;

    private int markReadPosition = -1;

    private int markCurReadBufferIndex = -1;

    private int markReadPosInCurBuffer = -1;


    private TieredFileSegmentInputStream(TieredFileSegment.FileSegmentType fileType, List<ByteBuffer> uploadBufferList, int contentLength) {
        this.fileType = fileType;
        this.contentLength = contentLength;
        this.uploadBufferList = uploadBufferList;
        if (uploadBufferList.size() > 0) {
            this.curBuffer = uploadBufferList.get(curReadBufferIndex);
        }
    }

    @Override
    public boolean markSupported() {
        return true;
    }

    @Override
    public synchronized void mark(int ignore) {
        this.markReadPosition = readPosition;
        this.markCurReadBufferIndex = curReadBufferIndex;
        this.markReadPosInCurBuffer = readPosInCurBuffer;
    }

    @Override
    public synchronized void reset() throws IOException {
        if (this.markReadPosition == -1) {
            throw new IOException("mark not set");
        }
        this.readPosition = markReadPosition;
        this.curReadBufferIndex = markCurReadBufferIndex;
        this.readPosInCurBuffer = markReadPosInCurBuffer;
        if (this.curReadBufferIndex < uploadBufferList.size()) {
            this.curBuffer = uploadBufferList.get(curReadBufferIndex);
        }
    }

    @Override
    public int available() {
        return contentLength - readPosition;
    }

    public List<ByteBuffer> getUploadBufferList() {
        return uploadBufferList;
    }

    public ByteBuffer getCodaBuffer() {
        return null;
    }

    @Override
    public int read() {
        if (available() <= 0) {
            return -1;
        }
        readPosition++;
        if (readPosInCurBuffer >= curBuffer.remaining()) {
            curReadBufferIndex++;
            if (curReadBufferIndex >= uploadBufferList.size()) {
                return -1;
            }
            curBuffer = uploadBufferList.get(curReadBufferIndex);
            readPosInCurBuffer = 0;
        }
        return curBuffer.get(readPosInCurBuffer++) & 0xff;
    }

    private static class CommitLogInputStream extends TieredFileSegmentInputStream {

        /**
         * commitLogOffset is the real physical offset of the commitLog buffer which is being read
         */
        private long commitLogOffset;

        private final ByteBuffer codaBuffer;

        private final ByteBuffer commitLogOffsetBuffer = ByteBuffer.allocate(8);

        private long markCommitLogOffset = -1;

        public CommitLogInputStream(TieredFileSegment.FileSegmentType fileType, long startOffset,
                                    List<ByteBuffer> uploadBufferList, ByteBuffer codaBuffer, int contentLength) {
            super(fileType, uploadBufferList, contentLength);
            this.commitLogOffset = startOffset;
            this.commitLogOffsetBuffer.putLong(0, startOffset);
            this.codaBuffer = codaBuffer;
        }

        @Override
        public synchronized void mark(int ignore) {
            super.mark(ignore);
            this.markCommitLogOffset = commitLogOffset;
        }

        @Override
        public synchronized void reset() throws IOException {
            super.reset();
            this.commitLogOffset = markCommitLogOffset;
            commitLogOffsetBuffer.putLong(0, commitLogOffset);
        }

        @Override
        public ByteBuffer getCodaBuffer() {
            return this.codaBuffer;
        }

        @Override
        public int read() {
            if (available() <= 0) {
                return -1;
            }
            readPosition++;
            if (curReadBufferIndex >= uploadBufferList.size()) {
                return readCoda();
            }
            int res;
            if (readPosInCurBuffer >= curBuffer.remaining()) {
                curReadBufferIndex++;
                if (curReadBufferIndex >= uploadBufferList.size()) {
                    readPosInCurBuffer = 0;
                    return readCoda();
                }
                curBuffer = uploadBufferList.get(curReadBufferIndex);
                commitLogOffset += readPosInCurBuffer;
                readPosInCurBuffer = 0;
                commitLogOffsetBuffer.putLong(0, commitLogOffset);
            }
            if (readPosInCurBuffer >= MessageBufferUtil.PHYSICAL_OFFSET_POSITION && readPosInCurBuffer < MessageBufferUtil.SYS_FLAG_OFFSET_POSITION) {
                res = commitLogOffsetBuffer.get(readPosInCurBuffer - MessageBufferUtil.PHYSICAL_OFFSET_POSITION) & 0xff;
                readPosInCurBuffer++;
            } else {
                res = curBuffer.get(readPosInCurBuffer++) & 0xff;
            }
            return res;
        }

        private int readCoda() {
            if (codaBuffer == null || readPosInCurBuffer >= codaBuffer.remaining()) {
                return -1;
            }
            return codaBuffer.get(readPosInCurBuffer++) & 0xff;
        }
    }

    public static class Factory {
        public static TieredFileSegmentInputStream build(TieredFileSegment.FileSegmentType fileType,
                                                         long startOffset,
                                                         List<ByteBuffer> uploadBufferList,
                                                         ByteBuffer codaBuffer,
                                                         int contentLength) {
            if (fileType == TieredFileSegment.FileSegmentType.COMMIT_LOG) {
                return new CommitLogInputStream(fileType, startOffset, uploadBufferList, codaBuffer, contentLength);
            } else if (fileType == TieredFileSegment.FileSegmentType.CONSUME_QUEUE) {
                return new TieredFileSegmentInputStream(fileType, uploadBufferList, contentLength);
            } else if (fileType == TieredFileSegment.FileSegmentType.INDEX) {
                if (uploadBufferList.size() != 1) {
                    throw new IllegalArgumentException("uploadBufferList size in INDEX type input stream must be 1");
                }
                return new TieredFileSegmentInputStream(fileType, uploadBufferList, contentLength);
            } else {
                throw new IllegalArgumentException("fileType is not supported");
            }
        }
    }
}

