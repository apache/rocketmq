/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.alibaba.rocketmq.common.queue;

import com.alibaba.rocketmq.common.constant.LoggerName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Comparator;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.locks.ReentrantLock;


/**
 * thread safe
 *
 * @auther lansheng.zj
 */
public class ConcurrentTreeMap<K, V> {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.BrokerLoggerName);
    private final ReentrantLock lock;
    private TreeMap<K, V> tree;
    private RoundQueue<K> roundQueue;


    public ConcurrentTreeMap(int capacity, Comparator<? super K> comparator) {
        tree = new TreeMap<K, V>(comparator);
        roundQueue = new RoundQueue<K>(capacity);
        lock = new ReentrantLock(true);
    }


    public Map.Entry<K, V> pollFirstEntry() {
        lock.lock();
        try {
            return tree.pollFirstEntry();
        } finally {
            lock.unlock();
        }
    }


    public V putIfAbsentAndRetExsit(K key, V value) {
        lock.lock();
        try {
            if (roundQueue.put(key)) {
                V exsit = tree.get(key);
                if (null == exsit) {
                    tree.put(key, value);
                    exsit = value;
                }
                log.warn("putIfAbsentAndRetExsit success. {}", key);
                return exsit;
            }

            else {
                V exsit = tree.get(key);
                return exsit;
            }
        } finally {
            lock.unlock();
        }
    }

}
