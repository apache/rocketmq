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

package org.apache.rocketmq.store.ha.controller;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Predicate;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.utils.CheckpointFile;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;

/**
 * Cache for epochFile.
 * Mapping (Epoch -> StartOffset)
 */
public class EpochFileCache {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);
    private final ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    private final Lock readLock = this.readWriteLock.readLock();
    private final Lock writeLock = this.readWriteLock.writeLock();
    private final CheckpointFile<EpochEntry> checkpoint;
    private final TreeMap<Integer, EpochEntry> epochMap;

    public EpochFileCache(final String path) {
        this.epochMap = new TreeMap<>();
        this.checkpoint = new CheckpointFile<>(path, new EpochEntrySerializer());
    }


    public boolean initCacheFromFile() {
        this.writeLock.lock();
        try {
            this.epochMap.clear();
            final List<EpochEntry> entries = this.checkpoint.read();
            for (final EpochEntry entry : entries) {
                this.epochMap.put(entry.getEpoch(), entry);
            }
            return true;
        } catch (final IOException e) {
            log.error("Error happen when init epoch entries from epochFile", e);
            return false;
        } finally {
            this.writeLock.unlock();
        }
    }

    public void initCacheFromEntries(final List<EpochEntry> entries) {
        this.writeLock.lock();
        try {
            this.epochMap.clear();
            for (final EpochEntry entry : entries) {
                this.epochMap.put(entry.getEpoch(), entry);
            }
            flush();
        } finally {
             this.writeLock.unlock();
        }
    }

    public boolean appendEntry(final EpochEntry entry) {
        this.writeLock.lock();
        try {
            if (!this.epochMap.isEmpty()) {
                final EpochEntry lastEntry = this.epochMap.lastEntry().getValue();
                if (lastEntry.getEpoch() >= entry.getEpoch() || lastEntry.getStartOffset() >= entry.getStartOffset()) {
                    return false;
                }
            }
            this.epochMap.put(entry.getEpoch(), new EpochEntry(entry));
            flush();
            return true;
        } finally {
            this.writeLock.unlock();
        }
    }

    /**
     * Set endOffset for lastEpochEntry.
     */
    public void setLastEpochEntryEndOffset(final long endOffset) {
        this.writeLock.lock();
        try {
            if (!this.epochMap.isEmpty()) {
                final EpochEntry lastEntry = this.epochMap.lastEntry().getValue();
                if (lastEntry.getStartOffset() < endOffset) {
                    lastEntry.setEndOffset(endOffset);
                }
            }
        } finally {
            this.writeLock.unlock();
        }
    }

    public EpochEntry lastEntry() {
        this.readLock.lock();
        try {
            if (this.epochMap.isEmpty()) {
                return null;
            }
            return new EpochEntry(this.epochMap.lastEntry().getValue());
        } finally {
            this.readLock.unlock();
        }
    }

    public EpochEntry getEntry(final int epoch) {
        this.readLock.lock();
        try {
            if (this.epochMap.containsKey(epoch)) {
                final EpochEntry entry = this.epochMap.get(epoch);
                final EpochEntry result = new EpochEntry(entry);
                final Map.Entry<Integer, EpochEntry> nextEntry = this.epochMap.ceilingEntry(epoch + 1);
                if (nextEntry != null) {
                    result.setEndOffset(nextEntry.getValue().getStartOffset());
                }
                return result;
            }
            return null;
        } finally {
            this.readLock.unlock();
        }
    }

    /**
     * Find the consistentPoint between compareCache and local.
     *
     * @return the consistent offset
     */
    public long findConsistentPoint(final EpochFileCache compareCache) {
        this.readLock.lock();
        try {
            long consistentOffset = -1;
            final Iterator<Map.Entry<Integer, EpochEntry>> iter = reverseIter();
            while (iter.hasNext()) {
                final Map.Entry<Integer, EpochEntry> curLocalEntry = iter.next();
                final EpochEntry compareEntry = compareCache.getEntry(curLocalEntry.getKey());
                if (compareEntry != null && compareEntry.getStartOffset() == curLocalEntry.getValue().getStartOffset()) {
                    consistentOffset = Math.min(curLocalEntry.getValue().getEndOffset(), compareEntry.getEndOffset());
                    break;
                }
            }
            return consistentOffset;
        } finally {
            this.readLock.unlock();
        }
    }

    private Iterator<Map.Entry<Integer, EpochEntry>> reverseIter() {
        this.readLock.lock();
        try {
            final Map<Integer, EpochEntry> descendingMap = new TreeMap<>(this.epochMap).descendingMap();
            // Set end offset for each entry
            long preEntryStartOffset = -1L;
            for (Map.Entry<Integer, EpochEntry> entry : descendingMap.entrySet()) {
                if (preEntryStartOffset > 0) {
                    entry.getValue().setEndOffset(preEntryStartOffset);
                }
                preEntryStartOffset = entry.getValue().getStartOffset();
            }
            return descendingMap.entrySet().iterator();
        } finally {
            this.readLock.unlock();
        }
    }

    /**
     * Remove epochEntries with epoch >= truncateEpoch.
     */
    public void truncateFromEpoch(final int truncateEpoch) {
        doTruncate((entry) -> entry.getEpoch() >= truncateEpoch);
    }

    /**
     * Remove epochEntries with startOffset >= truncateOffset.
     */
    public void truncateFromOffset(final long truncateOffset) {
        doTruncate((entry) -> entry.getStartOffset() >= truncateOffset);
    }

    /**
     * Clear all epochEntries
     */
    public void clearAll() {
        doTruncate((entry) -> true);
    }

    private void doTruncate(final Predicate<EpochEntry> predict) {
        this.writeLock.lock();
        try {
            this.epochMap.entrySet().removeIf(entry -> predict.test(entry.getValue()));
            flush();
        } finally {
            this.writeLock.unlock();
        }
    }

    private void flush() {
        this.writeLock.lock();
        try {
            final ArrayList<EpochEntry> entries = new ArrayList<>(this.epochMap.values());
            try {
                this.checkpoint.write(entries);
            } catch (final IOException e) {
                log.error("Error happen when flush epochEntries to epochCheckpointFile", e);
            }
        } finally {
            this.writeLock.unlock();
        }
    }

    static class EpochEntrySerializer implements CheckpointFile.CheckpointSerializer<EpochEntry> {

        @Override
        public String toLine(EpochEntry entry) {
            if (entry != null) {
                return String.format("%d-%d", entry.getEpoch(), entry.getStartOffset());
            } else {
                return null;
            }
        }

        @Override
        public EpochEntry fromLine(String line) {
            final String[] arr = line.split("-");
            if (arr.length == 2) {
                final int epoch = Integer.parseInt(arr[0]);
                final long startOffset = Long.parseLong(arr[1]);
                return new EpochEntry(epoch, startOffset);
            }
            return null;
        }
    }
}
