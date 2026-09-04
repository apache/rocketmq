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

package org.apache.rocketmq.broker.lite;

import java.util.SortedMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;

import org.apache.commons.collections4.trie.PatriciaTrie;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.lite.LiteUtil;

/**
 * Global prefix index over lmqName, backed by {@link PatriciaTrie}.
 *
 * <p>A single instance is shared across all parentTopics: every lmqName starts with
 * {@link LiteUtil#LITE_TOPIC_PREFIX} followed by its parentTopic, so lmqs of the same
 * parentTopic form a contiguous subtree, and a prefix lookup only walks that subtree.
 *
 * <p>Used to accelerate prefix-subscription full dispatch in
 * {@link LiteEventDispatcher#doFullDispatchForClient(String, String)}.
 *
 * <p>A {@link ReadWriteLock} guards the trie; reads dominate writes by ~10000x in steady state.
 * Empty prefix / parentTopic is rejected to avoid an unintended full-table scan.
 */
public class LmqPrefixIndex {

    private final PatriciaTrie<Boolean> trie = new PatriciaTrie<>();
    private final ReadWriteLock rwLock = new ReentrantReadWriteLock();

    /**
     * Insert lmqName into the trie. Idempotent. Returns {@code true} if newly added.
     */
    public boolean add(String lmqName) {
        if (lmqName == null) {
            return false;
        }
        rwLock.writeLock().lock();
        try {
            return trie.put(lmqName, Boolean.TRUE) == null;
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    /**
     * Remove lmqName from the trie. Returns {@code true} if an entry was removed.
     */
    public boolean remove(String lmqName) {
        rwLock.writeLock().lock();
        try {
            return trie.remove(lmqName) != null;
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    /**
     * Iterate all lmqs whose name starts with the given lmqName prefix.
     * The visitor returns {@code false} to break iteration early.
     * Empty prefix is rejected to avoid a full scan.
     *
     * @return {@code true} if iteration completed; {@code false} on early break or invalid input.
     */
    public boolean forEachLmqByPrefix(String lmqPrefix, Function<String, Boolean> visitor) {
        if (StringUtils.isEmpty(lmqPrefix) || visitor == null) {
            return false;
        }
        rwLock.readLock().lock();
        try {
            SortedMap<String, Boolean> sub = trie.prefixMap(lmqPrefix);
            for (String lmqName : sub.keySet()) {
                if (!visitor.apply(lmqName)) {
                    return false;
                }
            }
        } finally {
            rwLock.readLock().unlock();
        }
        return true;
    }

    /**
     * Best-effort size / emptiness probes for monitoring; intentionally lock-free.
     */
    public boolean isEmpty() {
        return trie.isEmpty();
    }

    public int size() {
        return trie.size();
    }
}
