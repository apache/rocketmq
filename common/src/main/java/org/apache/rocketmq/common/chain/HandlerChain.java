package org.apache.rocketmq.common.chain;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

public class HandlerChain<T, R> {

    private List<Handler<T, R>> handlers;
    private Iterator<Handler<T, R>> iterator;

    public static <T, R> HandlerChain<T, R> of(Handler<T, R> handler) {
        HandlerChain<T, R> chain = new HandlerChain<>();
        chain.handlers = new LinkedList<>();
        chain.handlers.add(handler);
        return chain;
    }

    public HandlerChain<T, R> addNext(Handler<T, R> handler) {
        if (this.handlers == null) {
            this.handlers = new ArrayList<>();
        }
        this.handlers.add(handler);
        return this;
    }

    public R handle(T t) {
        if (iterator == null) {
            iterator = handlers.iterator();
        }
        if (iterator.hasNext()) {
            Handler<T, R> handler = iterator.next();
            return handler.handle(t, this);
        }
        return null;
    }
}
