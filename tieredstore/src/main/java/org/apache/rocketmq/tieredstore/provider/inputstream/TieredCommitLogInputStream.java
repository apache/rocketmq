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

package org.apache.rocketmq.tieredstore.provider.inputstream;

import org.apache.rocketmq.tieredstore.provider.TieredFileSegment;
import org.apache.rocketmq.tieredstore.util.MessageBufferUtil;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

public class TieredCommitLogInputStream extends TieredFileSegmentInputStream {

    /**
     * commitLogOffset is the real physical offset of the commitLog buffer which is being read
     */
    private long commitLogOffset;

    private final ByteBuffer codaBuffer;
    
    private long markCommitLogOffset = -1;

    public TieredCommitLogInputStream(TieredFileSegment.FileSegmentType fileType, long startOffset,
                                      List<ByteBuffer> uploadBufferList, ByteBuffer codaBuffer, int contentLength) {
        super(fileType, uploadBufferList, contentLength);
        this.commitLogOffset = startOffset;
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
        }
        if (readPosInCurBuffer >= MessageBufferUtil.PHYSICAL_OFFSET_POSITION && readPosInCurBuffer < MessageBufferUtil.SYS_FLAG_OFFSET_POSITION) {
            res = (int) ((commitLogOffset >> (8 * (MessageBufferUtil.SYS_FLAG_OFFSET_POSITION - readPosInCurBuffer - 1))) & 0xff);
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
