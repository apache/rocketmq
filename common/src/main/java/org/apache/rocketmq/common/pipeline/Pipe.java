package org.apache.rocketmq.common.pipeline;

public interface Pipe<T, R> {

    R doProcess(T t);
}
