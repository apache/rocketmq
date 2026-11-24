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

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.rocketmq.common.lite.LiteUtil;

/**
 * Manages tombstones for exclusive subscription eviction.
 * <p>
 * When a client is evicted from a liteTopic (exclusive mode), an entry is placed here
 * to prevent the evicted client from pulling messages until the tombstone is cleared
 * during the next full subscription sync or client removal.
 * <p>
 * Key format: clientId$lmqName
 */
public class ExclusiveEvictionTombstones {

    private static final char KEY_SEPARATOR = LiteUtil.SEPARATOR;

    private final Set<String> tombstones = ConcurrentHashMap.newKeySet();

    /**
     * Check whether a tombstone exists for the given client and lmqName.
     */
    public boolean contains(String clientId, String lmqName) {
        return tombstones.contains(buildKey(clientId, lmqName));
    }

    /**
     * Add a tombstone for the given client and lmqName.
     */
    public void add(String clientId, String lmqName) {
        tombstones.add(buildKey(clientId, lmqName));
    }

    /**
     * Remove the tombstone for the given client and lmqName, if present.
     */
    public void remove(String clientId, String lmqName) {
        tombstones.remove(buildKey(clientId, lmqName));
    }

    /**
     * Remove all tombstones belonging to the specified client.
     */
    public void removeAllOf(String clientId) {
        String prefix = clientId + KEY_SEPARATOR;
        tombstones.removeIf(key -> key.startsWith(prefix));
    }

    /**
     * For a given client, remove tombstones whose lmqName is NOT in the provided active set.
     * This is used during full subscription sync to clear stale tombstones.
     */
    public void removeStale(String clientId, Set<String> activeLmqNames) {
        String prefix = clientId + KEY_SEPARATOR;
        tombstones.removeIf(key -> {
            if (!key.startsWith(prefix)) {
                return false;
            }
            String lmqName = key.substring(prefix.length());
            return !activeLmqNames.contains(lmqName);
        });
    }

    /**
     * Return the current number of tombstones (for monitoring/testing).
     */
    public int size() {
        return tombstones.size();
    }

    private static String buildKey(String clientId, String lmqName) {
        return clientId + KEY_SEPARATOR + lmqName;
    }
}
