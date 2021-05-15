package org.apache.rocketmq.common.concurrent;

/**
 * @author ZhangZiCheng
 * @date 2021/05/12
 */
@FunctionalInterface
public interface Callback<T> {
    void call(T response);
}
