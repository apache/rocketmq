package com.alibaba.rocketmq.common.queue;

import java.util.LinkedList;
import java.util.Queue;


/**
 * not thread safe
 *
 * @auther lansheng.zj
 */
public class RoundQueue<E> {

    private Queue<E> queue;
    private int capacity;


    public RoundQueue(int capacity) {
        this.capacity = capacity;
        queue = new LinkedList<E>();
    }


    public boolean put(E e) {
        boolean ok = false;
        if (!queue.contains(e)) {
            if (queue.size() >= capacity) {
                queue.poll();
            }
            queue.add(e);
            ok = true;
        }

        return ok;
    }
}
