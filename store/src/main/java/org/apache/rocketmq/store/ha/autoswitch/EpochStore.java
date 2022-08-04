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

import java.util.List;
import org.apache.rocketmq.common.EpochEntry;

/**
 * Cache for epochFile. Mapping (Epoch -> StartOffset)
 */
public interface EpochStore {

    /**
     * Load information from the entry List, and the content is temporary
     */
    void initStateFromEntries(final List<EpochEntry> entries);

    /**
     * Set endOffset for lastEpochEntry.
     */
    void setLastEpochEntryEndOffset(final long endOffset);

    /**
     * Load information from the file, and the content will be persistent when it is modified
     */
    boolean initStateFromFile();

    /**
     * Append new epoch entry to entry list tail
     */
    boolean tryAppendEpochEntry(final EpochEntry entry);

    /**
     * Return the list of the entries as a deep copy
     */
    List<EpochEntry> getAllEntries();

    /**
     * Search epoch entry by epoch
     */
    EpochEntry findEpochEntryByEpoch(final long epoch);

    /**
     * Search epoch entry by epoch
     */
    EpochEntry findCeilingEntryByEpoch(final long epoch);

    /**
     * Search epoch entry by log offset
     */
    EpochEntry findEpochEntryByOffset(final long offset);

    /**
     * Return first entry
     */
    EpochEntry getFirstEntry();

    /**
     * Return last entry
     */
    EpochEntry getLastEntry();

    /**
     * Return last epoch in last entry
     */
    long getLastEpoch();

    /**
     * Find the consistentPoint between compareStore and local.
     */
    long findLastConsistentPoint(final EpochStore compareEpoch);

    /**
     * Remove epochEntries with endOffset <= truncateOffset.
     */
    void truncatePrefixByOffset(final long truncateOffset);

    /**
     * Remove epochEntries with epoch > truncateEpoch.
     */
    void truncateSuffixByEpoch(final long truncateEpoch);

    /**
     * Remove epochEntries with startOffset > truncateOffset.
     */
    void truncateSuffixByOffset(final long truncateOffset);
}
