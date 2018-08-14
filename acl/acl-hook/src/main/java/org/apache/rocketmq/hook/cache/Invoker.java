package org.apache.rocketmq.hook.cache;

public interface Invoker<T> {

    T invoke();
}