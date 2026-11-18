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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
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
 *
 * <p>Callbacks are never invoked while the index lock is held: a prefix traversal first copies the
 * matching keys under the read lock and only then runs the visitor. Holding the read lock across
 * caller code would block writers ({@link #add(String)} / {@link #remove(String)}) and would
 * self-deadlock any visitor that transitively mutates the index, because
 * {@link ReentrantReadWriteLock} cannot upgrade a read lock into a write lock.
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
    /**
     * Copy all lmq names that start with the given prefix into a new, independent list.
     *
     * <p>The copy is made while holding the read lock, so the lock hold time is proportional to
     * the number of matching keys and never to the work the caller does with them. Callers can
     * therefore run arbitrary logic, or even mutate the index, while iterating the result.
     *
     * @param lmqPrefix prefix of the lmq names to collect
     * @return the matching lmq names, never {@code null}
     */
    public List<String> snapshotByPrefix(String lmqPrefix) {
        if (StringUtils.isEmpty(lmqPrefix)) {
            return Collections.emptyList();
        }
        rwLock.readLock().lock();
        try {
            return new ArrayList<>(trie.prefixMap(lmqPrefix).keySet());
        } finally {
            rwLock.readLock().unlock();
        }
    }

    /**
     * Iterate all lmqs whose name starts with the given lmqName prefix.
     * The visitor returns {@code false} to break iteration early.
     * Empty prefix is rejected to avoid a full scan.
     *
     * <p>The visitor is caller code that may run for an arbitrarily long time and may itself call
     * {@link #add(String)} / {@link #remove(String)}; it is therefore always invoked on the
     * snapshot taken by {@link #snapshotByPrefix(String)} once the index lock has been released.
     * A visitor that mutates the index then neither blocks other writers nor disturbs this traversal.
     *
     * @return {@code true} if iteration completed; {@code false} on early break or invalid input.
     */
    public boolean forEachLmqByPrefix(String lmqPrefix, Function<String, Boolean> visitor) {
        if (StringUtils.isEmpty(lmqPrefix) || visitor == null) {
            return false;
        }
        for (String lmqName : snapshotByPrefix(lmqPrefix)) {
            if (!visitor.apply(lmqName)) {
                return false;
            }
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
