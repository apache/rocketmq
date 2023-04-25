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

package org.apache.rocketmq.tieredstore.provider.s3;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class S3FileSegmentMetadata {

    private final LinkedList<ChunkMetadata> chunks = new LinkedList<>();

    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    private final ReentrantReadWriteLock.ReadLock readLock = lock.readLock();

    private final ReentrantReadWriteLock.WriteLock writeLock = lock.writeLock();

    private volatile boolean isSealed = false;

    private ChunkMetadata segment;

    public S3FileSegmentMetadata() {
    }

    /**
     * Seek the chunks that need to be read, which is the intersection of the chunks and the range of [position, position + length)
     * @param position start position
     * @param length data length
     * @return the chunks that need to be read
     * @throws IndexOutOfBoundsException if position or length is negative or position
     */
    public List<ChunkMetadata> seek(long position, int length) throws IndexOutOfBoundsException {
        readLock.lock();
        try {
            long endPosition = position + length - 1;
            if (position < 0 || length < 0 || position < getStartPosition() || endPosition > getEndPosition()) {
                throw new IndexOutOfBoundsException("position: " + position + ", length: " + length + ", Metadata: start: " + getStartPosition() + ", end: " + getEndPosition());
            }
            List<ChunkMetadata> needChunks = new LinkedList<>();
            if (length == 0) return needChunks;
            if (segment != null) {
                needChunks.add(segment);
                return needChunks;
            }
            for (ChunkMetadata chunk : chunks) {
                if (endPosition < chunk.getStartPosition()) break;
                if (position > chunk.getEndPosition()) continue;
                if (position <= chunk.getEndPosition() || endPosition >= chunk.getStartPosition()) {
                    needChunks.add(chunk);
                }
            }
            return needChunks;
        } finally {
            readLock.unlock();
        }
    }

    public boolean addChunk(ChunkMetadata chunk) {
        this.writeLock.lock();
        try {
            if (chunks.size() == 0 && chunk.getStartPosition() != 0) {
                return false;
            }
            if (chunks.size() > 0 && chunks.getLast().getEndPosition() + 1 != chunk.getStartPosition()) {
                return false;
            }
            chunks.addLast(chunk);
            return true;
        } finally {
            this.writeLock.unlock();
        }
    }

    public void setSegment(ChunkMetadata segment) {
        this.writeLock.lock();
        try {
            this.isSealed = true;
            this.segment = segment;
        } finally {
            this.writeLock.unlock();
        }
    }

    public void removeAllChunks() {
        this.writeLock.lock();
        try {
            this.chunks.clear();
        } finally {
            this.writeLock.unlock();
        }
    }

    public long getStartPosition() {
        this.readLock.lock();
        try {
            if (segment != null) return segment.getStartPosition();
            if (chunks.size() == 0) return -1;
            return chunks.getFirst().getStartPosition();
        } finally {
            this.readLock.unlock();
        }
    }

    public long getEndPosition() {
        this.readLock.lock();
        try {
            if (segment != null) return segment.getEndPosition();
            if (chunks.size() == 0) return -1;
            return chunks.getLast().getEndPosition();
        } finally {
            this.readLock.unlock();
        }
    }

    public long getSize() {
        long start = getStartPosition();
        long end = getEndPosition();
        if (start == -1) return 0;
        return end - start + 1;
    }

    public void clear() {
        this.writeLock.lock();
        try {
            chunks.clear();
            segment = null;
        } finally {
            this.writeLock.unlock();
        }
    }

    public long getChunkCount() {
        this.readLock.lock();
        try {
            return chunks.size();
        } finally {
            this.readLock.unlock();
        }
    }

    public boolean isSealed() {
        return isSealed;
    }

    public List<ChunkMetadata> getChunks() {
        this.readLock.lock();
        try {
            return new ArrayList<>(chunks);
        } finally {
            this.readLock.unlock();
        }
    }

    public void setSealed(boolean sealed) {
        this.isSealed = sealed;
    }
}
