package org.apache.rocketmq.common.pipeline;

public abstract class DefaultPipe<T, R> implements Pipe<T, R> {

    private DefaultPipe<T, R> next;

    public DefaultPipe<T, R> addNext(DefaultPipe<T, R> next) {
        this.next = next;
        return next;
    }

    public boolean hasNext() {
        return this.next != null;
    }

    public R doNext(T t) {
        if (next == null) {
            return null;
        }
        return next.doProcess(t);
    }
}
