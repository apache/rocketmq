package org.apache.rocketmq.common.chain;

public interface Handler<T, R> {

    R handle(T t, HandlerChain<T, R> chain);
}
