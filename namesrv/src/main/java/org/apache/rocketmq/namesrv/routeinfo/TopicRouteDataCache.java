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
package org.apache.rocketmq.namesrv.routeinfo;

import com.alibaba.fastjson2.JSONWriter;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.remoting.protocol.route.TopicRouteData;

/**
 * Only queried topics consume cache space. Invalidation changes a revision rather than
 * walking the cache or rebuilding routes on the broker registration thread.
 */
class TopicRouteDataCache {
    private final ConcurrentMap<String, Entry> cache = new ConcurrentHashMap<>();
    // Only fills touch eviction bookkeeping; hits never enqueue access records.
    private final Map<String, Entry> insertionOrder = new LinkedHashMap<>();
    private long weight;
    private final int maxBytes;

    TopicRouteDataCache(int maxBytes) {
        this.maxBytes = Math.max(0, maxBytes);
    }

    boolean isEnabled() {
        return maxBytes > 0;
    }

    byte[] get(String topic, long revision, boolean actingMaster, String orderTopicConf, boolean standardJson) {
        Entry entry = cache.get(topic);
        if (entry == null || entry.revision != revision || entry.actingMaster != actingMaster
            || !Objects.equals(entry.orderTopicConf, orderTopicConf)
            || System.nanoTime() - entry.createdNanos >= TimeUnit.MINUTES.toNanos(5)) {
            return null;
        }
        return standardJson ? entry.standardBody : entry.legacyBody;
    }

    synchronized void put(String topic, long revision, boolean actingMaster, TopicRouteData route, byte[] legacyBody, byte[] standardBody) {
        Entry current = cache.get(topic);
        if (current != null && current.revision == revision && current.actingMaster == actingMaster
            && Objects.equals(current.orderTopicConf, route.getOrderTopicConf())) {
            legacyBody = legacyBody == null ? current.legacyBody : legacyBody;
            standardBody = standardBody == null ? current.standardBody : standardBody;
        }
        Entry entry = new Entry(revision, actingMaster, route.getOrderTopicConf(), legacyBody, standardBody);
        // Oversized responses remain uncached, including responses larger than an int-sized weight.
        if (entry.weight(topic) <= maxBytes) {
            Entry previous = insertionOrder.remove(topic);
            if (previous != null) {
                weight -= previous.weight(topic);
            }
            insertionOrder.put(topic, entry);
            weight += entry.weight(topic);
            cache.put(topic, entry);
            Iterator<Map.Entry<String, Entry>> iterator = insertionOrder.entrySet().iterator();
            while (weight > maxBytes && iterator.hasNext()) {
                Map.Entry<String, Entry> eldest = iterator.next();
                weight -= eldest.getValue().weight(eldest.getKey());
                cache.remove(eldest.getKey(), eldest.getValue());
                iterator.remove();
            }
        }
    }

    static byte[] encode(TopicRouteData route, boolean standardJson) {
        if (standardJson) {
            return route.encode(JSONWriter.Feature.BrowserCompatible, JSONWriter.Feature.MapSortField);
        }
        return route.encode();
    }

    private static class Entry {
        private final long revision;
        private final boolean actingMaster;
        private final String orderTopicConf;
        private final byte[] legacyBody;
        private final byte[] standardBody;
        private final long createdNanos = System.nanoTime();

        Entry(long revision, boolean actingMaster, String orderTopicConf, byte[] legacyBody, byte[] standardBody) {
            this.revision = revision;
            this.actingMaster = actingMaster;
            this.orderTopicConf = orderTopicConf;
            this.legacyBody = legacyBody;
            this.standardBody = standardBody;
        }

        long weight(String topic) {
            return 256L + topic.length() * 2L + (orderTopicConf == null ? 0 : orderTopicConf.length() * 2L)
                + (legacyBody == null ? 0 : legacyBody.length) + (standardBody == null ? 0 : standardBody.length);
        }
    }
}
