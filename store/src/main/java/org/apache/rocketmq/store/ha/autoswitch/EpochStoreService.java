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
public class EpochStoreService implements EpochStore {

    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    private final ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    private final Lock readLock = this.readWriteLock.readLock();
    private final Lock writeLock = this.readWriteLock.writeLock();
    private final TreeMap<Long, EpochEntry> epochMap;
    private final CheckpointFile<EpochEntry> checkpoint;

    public EpochStoreService() {
        this.epochMap = new TreeMap<>();
        this.checkpoint = null;
    }

    public EpochStoreService(final String filePath) {
        this.epochMap = new TreeMap<>();
        this.checkpoint = new CheckpointFile<>(filePath, new EpochEntrySerializer());
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

    @Override
    public boolean initStateFromFile() {
        if (checkpoint == null) {
            return false;
        }

        this.writeLock.lock();
        try {
            final List<EpochEntry> entries = this.checkpoint.read();
            initEntries(entries);
            return true;
        } catch (final IOException e) {
            log.error("Error happened when init epoch entries from epochFile", e);
            return false;
        } finally {
            this.writeLock.unlock();
        }
    }

    @Override
    public void initStateFromEntries(final List<EpochEntry> entries) {
        this.writeLock.lock();
        try {
            initEntries(entries);
            flushCheckpoint();
        } finally {
            this.writeLock.unlock();
        }
    }

    @Override
    public void setLastEpochEntryEndOffset(long endOffset) {
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

    @Override
    public boolean tryAppendEpochEntry(final EpochEntry entry) {
        this.writeLock.lock();
        try {
            if (!this.epochMap.isEmpty()) {
                final EpochEntry lastEntry = this.epochMap.lastEntry().getValue();
                if (lastEntry.getEpoch() >= entry.getEpoch()
                    || lastEntry.getStartOffset() >= entry.getStartOffset()) {
                    log.error("The appending entry's is not latest, " +
                        "so append failed, last={}, append={}", entry, lastEntry);
                    return false;
                }
                lastEntry.setEndOffset(entry.getStartOffset());
            }
            this.epochMap.put(entry.getEpoch(), new EpochEntry(entry));
            flushCheckpoint();
            log.info("Append new epoch entry, current epoch size={}, new entry={}", this.epochMap.size(), entry);
            return true;
        } finally {
            this.writeLock.unlock();
        }
    }

    @Override
    public EpochEntry getFirstEntry() {
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

    @Override
    public EpochEntry getLastEntry() {
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

    @Override
    public long getLastEpoch() {
        final EpochEntry entry = getLastEntry();
        return entry != null ? entry.getEpoch() : -1L;
    }

    @Override
    public EpochEntry findEpochEntryByEpoch(final long epoch) {
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

    @Override
    public EpochEntry findEpochEntryByOffset(final long offset) {
        this.readLock.lock();
        try {
            if (!this.epochMap.isEmpty()) {
                for (Map.Entry<Long, EpochEntry> entry : this.epochMap.entrySet()) {
                    // start offset <= offset < end offset
                    if (entry.getValue().getStartOffset() <= offset && offset < entry.getValue().getEndOffset()) {
                        return new EpochEntry(entry.getValue());
                    }
                }
            }
            return null;
        } finally {
            this.readLock.unlock();
        }
    }

    @Override
    public EpochEntry findCeilingEntryByEpoch(long epoch) {
        this.readLock.lock();
        try {
            final Map.Entry<Long, EpochEntry> entry = this.epochMap.ceilingEntry(epoch + 1L);
            if (entry != null) {
                return new EpochEntry(entry.getValue());
            }
            return null;
        } finally {
            this.readLock.unlock();
        }
    }

    @Override
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

    @Override
    public long findLastConsistentPoint(final EpochStore compareEpoch) {
        this.readLock.lock();
        try {
            long consistentOffset = -1L;
            final Map<Long, EpochEntry> descendingMap = new TreeMap<>(this.epochMap).descendingMap();
            for (Map.Entry<Long, EpochEntry> curLocalEntry : descendingMap.entrySet()) {
                final EpochEntry compareEntry = compareEpoch.findEpochEntryByEpoch(curLocalEntry.getKey());
                if (compareEntry != null &&
                    compareEntry.getStartOffset() == curLocalEntry.getValue().getStartOffset()) {
                    consistentOffset = Math.min(curLocalEntry.getValue().getEndOffset(), compareEntry.getEndOffset());
                    break;
                }
            }
            return consistentOffset;
        } finally {
            this.readLock.unlock();
        }
    }

    @Override
    public void truncateSuffixByEpoch(final long truncateEpoch) {
        Predicate<EpochEntry> predict = (entry) -> entry.getEpoch() > truncateEpoch;
        doTruncateSuffix(predict);
    }

    @Override
    public void truncateSuffixByOffset(final long truncateOffset) {
        Predicate<EpochEntry> predict = (entry) -> entry.getStartOffset() > truncateOffset;
        doTruncateSuffix(predict);
    }

    private void doTruncateSuffix(Predicate<EpochEntry> predict) {
        this.writeLock.lock();
        try {
            this.epochMap.entrySet().removeIf(entry -> predict.test(entry.getValue()));
            if (this.epochMap.isEmpty()) {
                return;
            }
            final EpochEntry entry = this.epochMap.lastEntry().getValue();
            if (entry != null) {
                entry.setEndOffset(Long.MAX_VALUE);
            }
            flushCheckpoint();
        } finally {
            this.writeLock.unlock();
        }
    }

    @Override
    public void truncatePrefixByOffset(final long truncateOffset) {
        Predicate<EpochEntry> predict = (entry) -> entry.getEndOffset() <= truncateOffset;
        this.writeLock.lock();
        try {
            this.epochMap.entrySet().removeIf(entry -> predict.test(entry.getValue()));
            flushCheckpoint();
        } finally {
            this.writeLock.unlock();
        }
    }

    private void flushCheckpoint() {
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
                final long epoch = Long.parseLong(arr[0]);
                final long startOffset = Long.parseLong(arr[1]);
                return new EpochEntry(epoch, startOffset);
            }
            return null;
        }
    }
}
