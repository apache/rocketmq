/**
 * $Id: DataVersion.java 1831 2013-05-16 01:39:51Z shijia.wxr $
 */
package com.alibaba.rocketmq.common;

import java.util.concurrent.atomic.AtomicLong;

import com.alibaba.rocketmq.remoting.protocol.RemotingSerializable;


/**
 * 用来标识数据的版本号
 * 
 * @author shijia.wxr<vintage.wang@gmail.com>
 * 
 */
public class DataVersion extends RemotingSerializable {
    private long timestatmp = System.currentTimeMillis();
    private AtomicLong counter = new AtomicLong(0);


    public void assignNewOne(final DataVersion dataVersion) {
        this.timestatmp = dataVersion.timestatmp;
        this.counter.set(dataVersion.counter.get());
    }


    public void nextVersion() {
        this.timestatmp = System.currentTimeMillis();
        this.counter.incrementAndGet();
    }


    public long getTimestatmp() {
        return timestatmp;
    }


    public void setTimestatmp(long timestatmp) {
        this.timestatmp = timestatmp;
    }


    public AtomicLong getCounter() {
        return counter;
    }


    public void setCounter(AtomicLong counter) {
        this.counter = counter;
    }


    @Override
    public boolean equals(Object obj) {
        DataVersion dv = (DataVersion) obj;
        return this.timestatmp == dv.timestatmp && this.counter.get() == dv.counter.get();
    }
}
