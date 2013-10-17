package com.alibaba.rocketmq.test.integration.benchmark;

import java.util.concurrent.atomic.AtomicLong;


/**
 * description
 * 
 * @author: manhong.yqd<jodie.yqd@gmail.com>
 * @since: 13-9-5
 */
public class StatsBenchmarkProducer {
    // 1
    private final AtomicLong sendRequestSuccessCount = new AtomicLong(0L);
    // 2
    private final AtomicLong sendRequestFailedCount = new AtomicLong(0L);
    // 3
    private final AtomicLong receiveResponseSuccessCount = new AtomicLong(0L);
    // 4
    private final AtomicLong receiveResponseFailedCount = new AtomicLong(0L);
    // 5
    private final AtomicLong sendMessageSuccessTimeTotal = new AtomicLong(0L);
    // 6
    private final AtomicLong sendMessageMaxRT = new AtomicLong(0L);


    public Long[] createSnapshot() {
        Long[] snap = new Long[] {//
                System.currentTimeMillis(),//
                        this.sendRequestSuccessCount.get(),//
                        this.sendRequestFailedCount.get(),//
                        this.receiveResponseSuccessCount.get(),//
                        this.receiveResponseFailedCount.get(),//
                        this.sendMessageSuccessTimeTotal.get(), //
                };

        return snap;
    }


    public AtomicLong getSendRequestSuccessCount() {
        return sendRequestSuccessCount;
    }


    public AtomicLong getSendRequestFailedCount() {
        return sendRequestFailedCount;
    }


    public AtomicLong getReceiveResponseSuccessCount() {
        return receiveResponseSuccessCount;
    }


    public AtomicLong getReceiveResponseFailedCount() {
        return receiveResponseFailedCount;
    }


    public AtomicLong getSendMessageSuccessTimeTotal() {
        return sendMessageSuccessTimeTotal;
    }


    public AtomicLong getSendMessageMaxRT() {
        return sendMessageMaxRT;
    }
}
