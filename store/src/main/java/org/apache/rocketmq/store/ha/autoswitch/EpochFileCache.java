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
import java.io.UncheckedIOException;
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
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.protocol.EpochEntry;

/**
 * Cache for epochFile. Mapping (Epoch -> StartOffset)
 */
public class EpochFileCache {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);
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
            final List<EpochEntry> entries = validatedEntries(this.checkpoint.readStrict());
            publishEntries(entries);
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
            replaceEntries(entries);
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to initialize epoch entries", e);
        } finally {
            this.writeLock.unlock();
        }
    }

    private List<EpochEntry> validatedEntries(final List<EpochEntry> entries) throws IOException {
        final List<EpochEntry> result = new ArrayList<>(entries.size());
        EpochEntry preEntry = null;
        for (final EpochEntry source : entries) {
            if (source == null || source.getEpoch() < 0 || source.getStartOffset() < 0
                || source.getEndOffset() < source.getStartOffset()) {
                throw new IOException("Invalid epoch entry: " + source);
            }
            final EpochEntry entry = new EpochEntry(source);
            if (preEntry != null) {
                if (preEntry.getEpoch() >= entry.getEpoch() || preEntry.getStartOffset() >= entry.getStartOffset()) {
                    throw new IOException("Epoch entries must have increasing epochs and start offsets");
                }
                preEntry.setEndOffset(entry.getStartOffset());
            }
            result.add(entry);
            preEntry = entry;
        }
        return result;
    }

    private void publishEntries(final List<EpochEntry> entries) {
        this.epochMap.clear();
        for (final EpochEntry entry : entries) {
            this.epochMap.put(entry.getEpoch(), entry);
        }
    }

    private void replaceEntries(final List<EpochEntry> entries) throws IOException {
        final List<EpochEntry> replacement = validatedEntries(entries);
        if (this.checkpoint != null) {
            this.checkpoint.writeAtomically(replacement);
        }
        // Do not expose a history boundary until its durable replacement has succeeded.
        publishEntries(replacement);
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
            }
            final List<EpochEntry> replacement = getAllEntries();
            replacement.add(entry);
            replaceEntries(replacement);
            return true;
        } catch (IOException e) {
            log.error("Failed to persist appended epoch entry {}", entry, e);
            return false;
        } finally {
            this.writeLock.unlock();
        }
    }

    /**
     * Durably close the recovered history and install a controller-granted master epoch. Entries
     * starting at or beyond the recovered end have no surviving data and are replaced together
     * with the new boundary. Repeated grants within one startup keep an existing valid start.
     */
    public EpochEntry prepareMasterEpoch(final int epoch, final long recoveredMax) throws IOException {
        this.writeLock.lock();
        try {
            if (epoch < 0 || recoveredMax < 0 || epoch < lastEpoch()) {
                throw new IOException("Invalid master epoch or recovered offset: " + epoch + ", " + recoveredMax);
            }
            final EpochEntry lastEntry = lastEntry();
            if (lastEntry != null && lastEntry.getEpoch() == epoch && lastEntry.getStartOffset() <= recoveredMax) {
                replaceEntries(getAllEntries());
                return lastEntry;
            }
            final List<EpochEntry> replacement = getAllEntries();
            replacement.removeIf(entry -> entry.getStartOffset() >= recoveredMax);
            final EpochEntry newEntry = new EpochEntry(epoch, recoveredMax);
            replacement.add(newEntry);
            replaceEntries(replacement);
            return new EpochEntry(newEntry);
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
    public boolean truncateSuffixByEpoch(final int truncateEpoch) {
        Predicate<EpochEntry> predict = entry -> entry.getEpoch() >= truncateEpoch;
        return doTruncateSuffix(predict);
    }

    /**
     * Remove epochEntries with startOffset >= truncateOffset.
     */
    public boolean truncateSuffixByOffset(final long truncateOffset) {
        Predicate<EpochEntry> predict = entry -> entry.getStartOffset() >= truncateOffset;
        return doTruncateSuffix(predict);
    }

    private boolean doTruncateSuffix(Predicate<EpochEntry> predict) {
        this.writeLock.lock();
        try {
            final List<EpochEntry> replacement = getAllEntries();
            replacement.removeIf(predict);
            if (!replacement.isEmpty()) {
                replacement.get(replacement.size() - 1).setEndOffset(Long.MAX_VALUE);
            }
            replaceEntries(replacement);
            return true;
        } catch (IOException e) {
            log.error("Failed to persist truncated epoch suffix", e);
            return false;
        } finally {
            this.writeLock.unlock();
        }
    }

    /**
     * Remove epochEntries with endOffset <= truncateOffset.
     */
    public boolean truncatePrefixByOffset(final long truncateOffset) {
        Predicate<EpochEntry> predict = entry -> entry.getEndOffset() <= truncateOffset;
        this.writeLock.lock();
        try {
            final List<EpochEntry> replacement = getAllEntries();
            replacement.removeIf(predict);
            replaceEntries(replacement);
            return true;
        } catch (final IOException e) {
            log.error("Failed to persist truncated epoch prefix", e);
            return false;
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
