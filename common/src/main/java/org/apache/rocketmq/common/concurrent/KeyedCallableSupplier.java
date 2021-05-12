package org.apache.rocketmq.common.concurrent;

/**
 * 结合{@link KeyedSupplier}和{@link CallableSupplier}的优点
 *
 * @author zhangzicheng
 * @date 2021/05/12
 */
public interface KeyedCallableSupplier<K, V> extends KeyedSupplier<K, V>, CallableSupplier<V> {
}
