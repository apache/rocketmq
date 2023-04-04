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

/**
 * Metadata of a chunk in S3.
 *
 * <p>
 * There are two types of chunks in S3:
 * <ul>
 *     <li>Normal chunk, represents a normal chunk in S3, which size is usually less than {@link org.apache.rocketmq.tieredstore.common.TieredMessageStoreConfig#tieredStoreGroupCommitSize}
 *     <li>Segment chunk, means that this all normal chunks in one logic segment have been merged into a single chunk, which is named as segment chunk,
 *     which size is usually equals to {@link org.apache.rocketmq.tieredstore.common.TieredMessageStoreConfig#tieredStoreCommitLogMaxSize} or {@link org.apache.rocketmq.tieredstore.common.TieredMessageStoreConfig#tieredStoreConsumeQueueMaxSize}
 * </ul>
 * Once a segment chunk is created, it will never be changed, and we should delete all normal chunks in this segment.
 */
public class ChunkMetadata {

    /**
     * Name of the chunk in S3. Format:
     * <p>
     * Chunk:
     * <pre>
     *     {@link S3FileSegment#storePath}/chunk/chunk-${startPosition}
     * </pre>
     * <p>
     * Segment:
     * <pre>
     *     {@link S3FileSegment#storePath}/segment/segment-${startPosition}
     * </pre>
     */
    private String chunkName;

    private long startPosition;

    private int chunkSize;

    private boolean isSegmentType;

    public ChunkMetadata() {

    }

    public ChunkMetadata(String chunkName, long startPosition, int chunkSize) {
        this.startPosition = startPosition;
        this.chunkName = chunkName;
        this.chunkSize = chunkSize;
        this.isSegmentType = this.chunkName.contains("segment");
    }

    public int getChunkSize() {
        return chunkSize;
    }

    public String getChunkName() {
        return chunkName;
    }

    public long getStartPosition() {
        return startPosition;
    }

    public void setChunkName(String chunkName) {
        this.chunkName = chunkName;
    }

    public void setStartPosition(long startPosition) {
        this.startPosition = startPosition;
    }

    public void setChunkSize(int chunkSize) {
        this.chunkSize = chunkSize;
    }

    public long getEndPosition() {
        return startPosition + chunkSize - 1;
    }

    public boolean isSegmentType() {
        return isSegmentType;
    }

    @Override
    public String toString() {
        return "ChunkMetadata{" +
                "chunkName='" + chunkName + '\'' +
                ", startPosition=" + startPosition +
                ", endPosition=" + getEndPosition() +
                '}';
    }
}
