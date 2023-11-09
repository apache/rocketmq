package org.apache.rocketmq.common.pipeline;

public interface Pipeline<T, R> {

    R process(T t);
}
