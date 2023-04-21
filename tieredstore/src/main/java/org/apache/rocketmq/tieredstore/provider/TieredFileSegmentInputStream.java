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
import java.util.Arrays;
import java.util.List;
public abstract class TieredFileSegmentInputStream extends InputStream {

    private final TieredFileSegment.FileSegmentType fileType;
    private final int contentLength;

    /**
     * readPosition is the now position in the stream
     */
    private int readPosition = 0;

    private int markReadPosition = -1;

    public static TieredFileSegmentInputStream buildTieredFileSegmentInputStream(TieredFileSegment.FileSegmentType fileType,
                                                                                long startOffset,
                                                                                List<ByteBuffer> uploadBufferList,
                                                                                ByteBuffer codaBuffer,
                                                                                int contentLength) {
        if (fileType == TieredFileSegment.FileSegmentType.COMMIT_LOG) {
            return new CommitLogInputStream(fileType, startOffset, uploadBufferList, codaBuffer, contentLength);
        } else if (fileType == TieredFileSegment.FileSegmentType.CONSUME_QUEUE) {
            return new ConsumeQueueInputStream(fileType, uploadBufferList, contentLength);
        } else if (fileType == TieredFileSegment.FileSegmentType.INDEX) {
            if (uploadBufferList.size() != 1) {
                throw new IllegalArgumentException("uploadBufferList size in INDEX type input stream must be 1");
            }
            return new IndexInputStream(fileType, uploadBufferList.get(0), contentLength);
        } else {
            throw new IllegalArgumentException("fileType is not supported");
        }
    }

    protected TieredFileSegmentInputStream(TieredFileSegment.FileSegmentType fileType, int contentLength) {
        this.fileType = fileType;
        this.contentLength = contentLength;
    }

    @Override
    public boolean markSupported() {
        return true;
    }

    @Override
    public synchronized void mark(int ignore) {
        this.markReadPosition = readPosition;
        realMark();
    }

    @Override
    public synchronized void reset() throws IOException {
        if (this.markReadPosition == -1) {
            throw new IOException("mark not set");
        }
        this.readPosition = markReadPosition;
        realReset();
    }

    @Override
    public int available() {
        return contentLength - readPosition;
    }

    public abstract int realRead();

    public abstract void realMark();

    public abstract void realReset();

    public abstract List<ByteBuffer> getUploadBufferList();

    public abstract ByteBuffer getCodaBuffer();

    @Override
    public int read() {
        if (readPosition >= contentLength) return -1;
        readPosition++;
        return realRead();
    }

    private static class CommitLogInputStream extends TieredFileSegmentInputStream {

        /**
         * curReadBufferIndex is the index of the buffer in uploadBufferList which is being read
         */
        private int curReadBufferIndex = 0;
        /**
         * readPosInCurBuffer is the position in the buffer which is being read
         */
        private int readPosInCurBuffer = 0;
        /**
         * commitLogOffset is the real physical offset of the commitLog buffer which is being read
         */
        private long commitLogOffset;

        private final List<ByteBuffer> uploadBufferList;

        private final ByteBuffer codaBuffer;

        private ByteBuffer curBuffer;

        private final ByteBuffer commitLogOffsetBuffer = ByteBuffer.allocate(8);

        private int markCurReadBufferIndex = -1;

        private int markReadPosInCurBuffer = -1;

        private long markCommitLogOffset = -1;

        public CommitLogInputStream(TieredFileSegment.FileSegmentType fileType, long startOffset,
                                    List<ByteBuffer> uploadBufferList, ByteBuffer codaBuffer, int contentLength) {
            super(fileType, contentLength);
            this.commitLogOffset = startOffset;
            this.commitLogOffsetBuffer.putLong(0, startOffset);
            this.uploadBufferList = uploadBufferList;
            this.codaBuffer = codaBuffer;
            if (uploadBufferList.size() > 0) {
                this.curBuffer = uploadBufferList.get(0);
            }
        }

        @Override
        public void realMark() {
            markCurReadBufferIndex = curReadBufferIndex;
            markReadPosInCurBuffer = readPosInCurBuffer;
            markCommitLogOffset = commitLogOffset;
        }

        @Override
        public void realReset() {
            curReadBufferIndex = markCurReadBufferIndex;
            readPosInCurBuffer = markReadPosInCurBuffer;
            commitLogOffset = markCommitLogOffset;
            if (curReadBufferIndex < uploadBufferList.size()) {
                curBuffer = uploadBufferList.get(curReadBufferIndex);
            }
            commitLogOffsetBuffer.putLong(0, commitLogOffset);
        }

        @Override
        public List<ByteBuffer> getUploadBufferList() {
            return this.uploadBufferList;
        }

        @Override
        public ByteBuffer getCodaBuffer() {
            return this.codaBuffer;
        }

        @Override
        public int realRead() {
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

    private static class ConsumeQueueInputStream extends TieredFileSegmentInputStream {

        /**
         * curReadBufferIndex is the index of the buffer in uploadBufferList which is being read
         */
        private int curReadBufferIndex = 0;
        /**
         * readPosInCurBuffer is the position in the buffer which is being read
         */
        private int readPosInCurBuffer = 0;

        private ByteBuffer curBuffer;

        private final List<ByteBuffer> uploadBufferList;

        private int markCurReadBufferIndex = -1;
        private int markReadPosInCurBuffer = -1;


        public ConsumeQueueInputStream(TieredFileSegment.FileSegmentType fileType, List<ByteBuffer> uploadBufferList, int contentLength) {
            super(fileType, contentLength);
            this.uploadBufferList = uploadBufferList;
            if (uploadBufferList.size() > 0) {
                this.curBuffer = uploadBufferList.get(0);
            }
        }

        @Override
        public int realRead() {
            if (curReadBufferIndex >= uploadBufferList.size()) {
                return -1;
            }
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

        @Override
        public void realMark() {
            this.markCurReadBufferIndex = curReadBufferIndex;
            this.markReadPosInCurBuffer = readPosInCurBuffer;
        }

        @Override
        public void realReset() {
            this.curReadBufferIndex = markCurReadBufferIndex;
            this.readPosInCurBuffer = markReadPosInCurBuffer;
            if (curReadBufferIndex < uploadBufferList.size()) {
                this.curBuffer = uploadBufferList.get(curReadBufferIndex);
            }
        }

        @Override
        public List<ByteBuffer> getUploadBufferList() {
            return this.uploadBufferList;
        }

        @Override
        public ByteBuffer getCodaBuffer() {
            return null;
        }
    }

    private static class IndexInputStream extends TieredFileSegmentInputStream {

        private final ByteBuffer curBuffer;

        private int readPosInCurBuffer = 0;

        private int markReadPosInCurBuffer = -1;

        public IndexInputStream(TieredFileSegment.FileSegmentType fileType, ByteBuffer curBuffer, int contentLength) {
            super(fileType, contentLength);
            if (curBuffer == null) {
                throw new IllegalArgumentException("curBuffer is null");
            }
            this.curBuffer = curBuffer;
        }

        @Override
        public int realRead() {
            if (readPosInCurBuffer >= curBuffer.remaining()) {
                return -1;
            }
            return curBuffer.get(readPosInCurBuffer++) & 0xff;
        }

        @Override
        public void realMark() {
            this.markReadPosInCurBuffer = readPosInCurBuffer;
        }

        @Override
        public void realReset() {
            this.readPosInCurBuffer = markReadPosInCurBuffer;
        }

        @Override
        public List<ByteBuffer> getUploadBufferList() {
            return Arrays.asList(curBuffer);
        }

        @Override
        public ByteBuffer getCodaBuffer() {
            return null;
        }
    }

}

