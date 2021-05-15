package org.apache.rocketmq.common.concurrent;

import java.util.function.Supplier;

/**
 * 弥补原生{@link Supplier}没有任务key的痛点
 *
 * @author ZhangZiZheng
 * @date 2021/05/12
 */
public interface KeyedSupplier<K, V> extends Supplier<V> {

    /**
     * 任务的key
     *
     * @return key
     */
    K key();
}
