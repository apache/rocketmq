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

import com.googlecode.concurrentlinkedhashmap.ConcurrentLinkedHashMap;
import com.googlecode.concurrentlinkedhashmap.EvictionListener;

public class ExpiredLocalCache<K, T> {
    private ConcurrentLinkedHashMap<K, CacheObject<T>> cache;
    private EvictionListener<K, CacheObject<T>> listener;

    public ExpiredLocalCache(int size) {
        cache = new ConcurrentLinkedHashMap.Builder<K, CacheObject<T>>().maximumWeightedCapacity(size).build();
    }

    public ExpiredLocalCache(int size, String name) {
        cache = new ConcurrentLinkedHashMap.Builder<K, CacheObject<T>>().maximumWeightedCapacity(size).build();
    }

    public ExpiredLocalCache(int size, boolean memoryMeter, EvictionListener<K, CacheObject<T>> listener) {
        this.listener = listener;
        cache = new ConcurrentLinkedHashMap.Builder<K, CacheObject<T>>().listener(listener).maximumWeightedCapacity(size).build();
    }

    public T get(K key) {
        CacheObject<T> object = cache.get(key);
        if (object == null) {
            return null;
        }
        T ret = object.getTarget();
        if (ret == null) {
            this.delete(key);
        }
        return ret;
    }

    public T put(K key, T v, long exp) {
        CacheObject<T> value = new CacheObject<T>(exp, v);
        CacheObject<T> old = cache.put(key, value);
        if (old == null) {
            return null;
        } else {
            return old.getTarget();
        }
    }

    public T putIfAbsent(K key, T v, long exp) {
        CacheObject<T> value = new CacheObject<T>(exp, v);
        CacheObject<T> old = cache.putIfAbsent(key, value);
        if (old == null) {
            return null;
        } else {
            return old.getTarget();
        }
    }

    public T delete(K key) {
        CacheObject<T> object = cache.remove(key);
        if (object == null) {
            return null;
        }
        T ret = object.getTarget();
        return ret;
    }

    public ConcurrentLinkedHashMap<K, CacheObject<T>> getCache() {
        return cache;
    }

}
