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
            // 已经存在
            else {
                V exsit = tree.get(key);
                return exsit;
            }
        } finally {
            lock.unlock();
        }
    }

}
