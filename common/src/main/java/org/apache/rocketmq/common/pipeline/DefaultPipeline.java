package org.apache.rocketmq.common.pipeline;

public class DefaultPipeline<T, R> implements Pipeline<T, R> {

    private DefaultPipe<T, R> head;

    public static <T, R> DefaultPipeline<T, R> of(DefaultPipe<T, R> pipe) {
        DefaultPipeline<T, R> pipeline = new DefaultPipeline<>();
        pipeline.head = pipe;
        return pipeline;
    }

    public DefaultPipeline<T, R> addNext(DefaultPipe<T, R> next) {
        if (this.head == null) {
            this.head = next;
        } else {
            this.head.addNext(next);
        }
        return this;
    }

    @Override
    public R process(T t) {
        return head.doProcess(t);
    }
}
