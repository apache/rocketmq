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
package org.apache.rocketmq.util.cache;

import java.util.LinkedHashMap;
import java.util.Map;

public class LocalCache<K, V> extends LinkedHashMap<K, V> {

    private static final long serialVersionUID = 1606231700062718297L;

    private static final int DEFAULT_CACHE_SIZE = 1000;

    private int cacheSize = DEFAULT_CACHE_SIZE;
    private CacheEvictHandler<K, V> handler;

    /**
     * The default initial capacity - MUST be a power of two.
     */
    static final int DEFAULT_INITIAL_CAPACITY = 1 << 4; // aka 16


    /**
     * The load factor used when none specified in constructor.
     */
    static final float DEFAULT_LOAD_FACTOR = 0.75f;

    public LocalCache(int cacheSize, boolean isLru, CacheEvictHandler<K, V> handler) {
        super(DEFAULT_INITIAL_CAPACITY, DEFAULT_LOAD_FACTOR, isLru);
        this.cacheSize = cacheSize;
        this.handler = handler;
    }


    @Override
    protected boolean removeEldestEntry(Map.Entry<K, V> eldest) {
        boolean result = this.size() > cacheSize;
        if (result && handler != null) {
            handler.onEvict(eldest);
        }
        return result;
    }

}
