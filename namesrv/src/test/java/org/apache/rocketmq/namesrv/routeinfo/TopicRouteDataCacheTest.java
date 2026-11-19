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
import org.apache.rocketmq.remoting.protocol.route.TopicRouteData;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class TopicRouteDataCacheTest {
    @Test
    public void testVariantsAndInvalidationInputs() {
        TopicRouteDataCache cache = new TopicRouteDataCache(4096);
        TopicRouteData route = new TopicRouteData();
        route.setOrderTopicConf("broker:4");
        byte[] legacy = route.encode();
        byte[] standard = route.encode(JSONWriter.Feature.BrowserCompatible, JSONWriter.Feature.MapSortField);
        cache.put("topic", 1, false, route, legacy, standard);
        assertSame(legacy, cache.get("topic", 1, false, "broker:4", false));
        assertSame(standard, cache.get("topic", 1, false, "broker:4", true));
        assertNull(cache.get("topic", 2, false, "broker:4", true));
        assertNull(cache.get("topic", 1, true, "broker:4", true));
        assertNull(cache.get("topic", 1, false, null, true));
        assertNull(cache.get("other", 1, false, "broker:4", true));
        assertArrayEquals(legacy, TopicRouteDataCache.encode(route, false));
        assertArrayEquals(standard, TopicRouteDataCache.encode(route, true));
    }

    @Test
    public void testOversizedAndDisabledCache() {
        TopicRouteData route = new TopicRouteData();
        for (int limit : new int[] {-1, 0, 1, 256}) {
            TopicRouteDataCache cache = new TopicRouteDataCache(limit);
            cache.put("topic", 1, false, route, new byte[256], new byte[256]);
            assertNull(cache.get("topic", 1, false, null, true));
        }
    }

    @Test
    public void testAggregateWeightIsBounded() {
        TopicRouteDataCache cache = new TopicRouteDataCache(4096);
        TopicRouteData route = new TopicRouteData();
        for (int i = 0; i < 100; i++) {
            cache.put("topic-" + i, 1, false, route, new byte[32], new byte[32]);
        }
        int retained = 0;
        for (int i = 0; i < 100; i++) {
            if (cache.get("topic-" + i, 1, false, null, true) != null) {
                retained++;
            }
        }
        assertTrue(retained > 0);
        assertTrue(retained <= 20);
    }

    @Test
    public void testLazyVariantsAreMergedOnlyWithinSameRevision() {
        TopicRouteDataCache cache = new TopicRouteDataCache(4096);
        TopicRouteData route = new TopicRouteData();
        byte[] standard = new byte[] {1};
        byte[] legacy = new byte[] {2};
        cache.put("topic", 1, false, route, null, standard);
        assertNull(cache.get("topic", 1, false, null, false));
        cache.put("topic", 1, false, route, legacy, null);
        assertSame(standard, cache.get("topic", 1, false, null, true));
        assertSame(legacy, cache.get("topic", 1, false, null, false));
        cache.put("topic", 2, false, route, legacy, null);
        assertNull(cache.get("topic", 2, false, null, true));
    }
}
