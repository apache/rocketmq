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

package org.apache.rocketmq.store.ha.autoswitch;

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
import org.apache.rocketmq.common.EpochEntry;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.utils.CheckpointFile;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;

/**
 * Cache for epochFile. Mapping (Epoch -> StartOffset)
 */
public class EpochFileCache {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);
    private final ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    private final Lock readLock = this.readWriteLock.readLock();
    private final Lock writeLock = this.readWriteLock.writeLock();
    private final TreeMap<Integer, EpochEntry> epochMap;
    private CheckpointFile<EpochEntry> checkpoint;

    public EpochFileCache() {
        this.epochMap = new TreeMap<>();
    }

    public EpochFileCache(final String path) {
        this.epochMap = new TreeMap<>();
        this.checkpoint = new CheckpointFile<>(path, new EpochEntrySerializer());
    }

    public boolean initCacheFromFile() {
        this.writeLock.lock();
        try {
            final List<EpochEntry> entries = this.checkpoint.read();
            initEntries(entries);
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
            initEntries(entries);
            flush();
        } finally {
            this.writeLock.unlock();
        }
    }

    private void initEntries(final List<EpochEntry> entries) {
        this.epochMap.clear();
        EpochEntry preEntry = null;
        for (final EpochEntry entry : entries) {
            this.epochMap.put(entry.getEpoch(), entry);
            if (preEntry != null) {
                preEntry.setEndOffset(entry.getStartOffset());
            }
            preEntry = entry;
        }
    }

    public int getEntrySize() {
        this.readLock.lock();
        try {
            return this.epochMap.size();
        } finally {
            this.readLock.unlock();
        }
    }

    public boolean appendEntry(final EpochEntry entry) {
        this.writeLock.lock();
        try {
            if (!this.epochMap.isEmpty()) {
                final EpochEntry lastEntry = this.epochMap.lastEntry().getValue();
                if (lastEntry.getEpoch() >= entry.getEpoch() || lastEntry.getStartOffset() >= entry.getStartOffset()) {
                    log.error("The appending entry's lastEpoch or endOffset {} is not bigger than lastEntry {}, append failed", entry, lastEntry);
                    return false;
                }
                lastEntry.setEndOffset(entry.getStartOffset());
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
                if (lastEntry.getStartOffset() <= endOffset) {
                    lastEntry.setEndOffset(endOffset);
                }
            }
        } finally {
            this.writeLock.unlock();
        }
    }

    public EpochEntry firstEntry() {
        this.readLock.lock();
        try {
            if (this.epochMap.isEmpty()) {
                return null;
            }
            return new EpochEntry(this.epochMap.firstEntry().getValue());
        } finally {
            this.readLock.unlock();
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

    public int lastEpoch() {
        final EpochEntry entry = lastEntry();
        if (entry != null) {
            return entry.getEpoch();
        }
        return -1;
    }

    public EpochEntry getEntry(final int epoch) {
        this.readLock.lock();
        try {
            if (this.epochMap.containsKey(epoch)) {
                final EpochEntry entry = this.epochMap.get(epoch);
                return new EpochEntry(entry);
            }
            return null;
        } finally {
            this.readLock.unlock();
        }
    }

    public EpochEntry findEpochEntryByOffset(final long offset) {
        this.readLock.lock();
        try {
            if (!this.epochMap.isEmpty()) {
                for (Map.Entry<Integer, EpochEntry> entry : this.epochMap.entrySet()) {
                    if (entry.getValue().getStartOffset() <= offset && entry.getValue().getEndOffset() > offset) {
                        return new EpochEntry(entry.getValue());
                    }
                }
            }
            return null;
        } finally {
            this.readLock.unlock();
        }
    }

    public EpochEntry nextEntry(final int epoch) {
        this.readLock.lock();
        try {
            final Map.Entry<Integer, EpochEntry> entry = this.epochMap.ceilingEntry(epoch + 1);
            if (entry != null) {
                return new EpochEntry(entry.getValue());
            }
            return null;
        } finally {
            this.readLock.unlock();
        }
    }

    public List<EpochEntry> getAllEntries() {
        this.readLock.lock();
        try {
            final ArrayList<EpochEntry> result = new ArrayList<>(this.epochMap.size());
            this.epochMap.forEach((key, value) -> result.add(new EpochEntry(value)));
            return result;
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
            final Map<Integer, EpochEntry> descendingMap = new TreeMap<>(this.epochMap).descendingMap();
            final Iterator<Map.Entry<Integer, EpochEntry>> iter = descendingMap.entrySet().iterator();
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

    /**
     * Remove epochEntries with epoch >= truncateEpoch.
     */
    public void truncateSuffixByEpoch(final int truncateEpoch) {
        Predicate<EpochEntry> predict = entry -> entry.getEpoch() >= truncateEpoch;
        doTruncateSuffix(predict);
    }

    /**
     * Remove epochEntries with startOffset >= truncateOffset.
     */
    public void truncateSuffixByOffset(final long truncateOffset) {
        Predicate<EpochEntry> predict = entry -> entry.getStartOffset() >= truncateOffset;
        doTruncateSuffix(predict);
    }

    private void doTruncateSuffix(Predicate<EpochEntry> predict) {
        this.writeLock.lock();
        try {
            this.epochMap.entrySet().removeIf(entry -> predict.test(entry.getValue()));
            final EpochEntry entry = lastEntry();
            if (entry != null) {
                entry.setEndOffset(Long.MAX_VALUE);
            }
            flush();
        } finally {
            this.writeLock.unlock();
        }
    }

    /**
     * Remove epochEntries with endOffset <= truncateOffset.
     */
    public void truncatePrefixByOffset(final long truncateOffset) {
        Predicate<EpochEntry> predict = entry -> entry.getEndOffset() <= truncateOffset;
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
            if (this.checkpoint != null) {
                final ArrayList<EpochEntry> entries = new ArrayList<>(this.epochMap.values());
                this.checkpoint.write(entries);
            }
        } catch (final IOException e) {
            log.error("Error happen when flush epochEntries to epochCheckpointFile", e);
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
