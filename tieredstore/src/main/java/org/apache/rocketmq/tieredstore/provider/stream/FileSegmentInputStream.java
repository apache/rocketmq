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

package org.apache.rocketmq.tieredstore.provider.stream;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.List;
import org.apache.commons.collections.CollectionUtils;
import org.apache.rocketmq.tieredstore.common.FileSegmentType;

public class FileSegmentInputStream extends InputStream {

    /**
     * file type, can be commitlog, consume queue or indexfile now
     */
    protected final FileSegmentType fileType;

    /**
     * hold bytebuffer
     */
    protected final List<ByteBuffer> bufferList;

    /**
     * total remaining of bytebuffer list
     */
    protected final int contentLength;

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

    public FileSegmentInputStream(
        FileSegmentType fileType, List<ByteBuffer> bufferList, int contentLength) {
        this.fileType = fileType;
        this.contentLength = contentLength;
        this.bufferList = bufferList;
        if (bufferList != null && bufferList.size() > 0) {
            this.curBuffer = bufferList.get(curReadBufferIndex);
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
        if (this.curReadBufferIndex < bufferList.size()) {
            this.curBuffer = bufferList.get(curReadBufferIndex);
        }
    }

    public synchronized void rewind() {
        this.readPosition = 0;
        this.curReadBufferIndex = 0;
        this.readPosInCurBuffer = 0;
        if (CollectionUtils.isNotEmpty(bufferList)) {
            this.curBuffer = bufferList.get(0);
            for (ByteBuffer buffer : bufferList) {
                buffer.rewind();
            }
        }
    }

    public int getContentLength() {
        return contentLength;
    }

    @Override
    public int available() {
        return contentLength - readPosition;
    }

    public List<ByteBuffer> getBufferList() {
        return bufferList;
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
            if (curReadBufferIndex >= bufferList.size()) {
                return -1;
            }
            curBuffer = bufferList.get(curReadBufferIndex);
            readPosInCurBuffer = 0;
        }
        return curBuffer.get(readPosInCurBuffer++) & 0xff;
    }

    @Override
    public int read(byte[] b, int off, int len) {
        if (b == null) {
            throw new NullPointerException();
        } else if (off < 0 || len < 0 || len > b.length - off) {
            throw new IndexOutOfBoundsException("off < 0 || len < 0 || len > b.length - off");
        }
        if (readPosition >= contentLength) {
            return -1;
        }

        int available = available();
        if (len > available) {
            len = available;
        }
        if (len <= 0) {
            return 0;
        }
        int needRead = len;
        int pos = readPosition;
        int bufIndex = curReadBufferIndex;
        int posInCurBuffer = readPosInCurBuffer;
        ByteBuffer curBuf = curBuffer;
        while (needRead > 0 && bufIndex < bufferList.size()) {
            curBuf = bufferList.get(bufIndex);
            int remaining = curBuf.remaining() - posInCurBuffer;
            int readLen = Math.min(remaining, needRead);
            // read from curBuf
            curBuf.position(posInCurBuffer);
            curBuf.get(b, off, readLen);
            curBuf.position(0);
            // update flags
            off += readLen;
            needRead -= readLen;
            pos += readLen;
            posInCurBuffer += readLen;
            if (posInCurBuffer == curBuf.remaining()) {
                // read from next buf
                bufIndex++;
                posInCurBuffer = 0;
            }
        }
        readPosition = pos;
        curReadBufferIndex = bufIndex;
        readPosInCurBuffer = posInCurBuffer;
        curBuffer = curBuf;
        return len;
    }
}

