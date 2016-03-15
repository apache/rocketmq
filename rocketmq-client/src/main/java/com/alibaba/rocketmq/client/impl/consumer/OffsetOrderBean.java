package com.alibaba.rocketmq.client.impl.consumer;

/**
 * Created by jodie on 16/3/13.
 */
public class OffsetOrderBean implements Comparable<OffsetOrderBean> {
    private long offset;
    private long timestamp;


    public OffsetOrderBean(final long offset) {
        this.offset = offset;
        this.timestamp = System.currentTimeMillis();
    }


    public long getOffset() {
        return offset;
    }


    public long getTimestamp() {
        return timestamp;
    }


    @Override
    public boolean equals(final Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        final OffsetOrderBean orderBean = (OffsetOrderBean) o;

        return offset == orderBean.offset;
    }


    @Override
    public int hashCode() {
        return (int) (offset ^ (offset >>> 32));
    }


    @Override
    public int compareTo(final OffsetOrderBean o) {
        return (int) (this.offset - o.getOffset());
    }
}
