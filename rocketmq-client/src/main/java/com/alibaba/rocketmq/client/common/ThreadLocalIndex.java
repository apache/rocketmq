package com.alibaba.rocketmq.client.common;

import java.util.Random;

public class ThreadLocalIndex {
    private final ThreadLocal<Integer> threadLocalIndex = new ThreadLocal<Integer>();

    public ThreadLocalIndex(int value) {

    }

    public int getAndIncrement() {
        Integer index = this.threadLocalIndex.get();
        if (null == index) {
            index = new Integer(Math.abs(new Random().nextInt()));
            if (index < 0) index = 0;
            this.threadLocalIndex.set(index);
        }

        index = Math.abs(index + 1);
        if (index < 0)
            index = 0;

        this.threadLocalIndex.set(index);
        return index;
    }
}
