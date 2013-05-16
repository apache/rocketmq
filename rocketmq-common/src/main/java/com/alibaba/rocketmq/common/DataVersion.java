/**
 * $Id: DataVersion.java 1831 2013-05-16 01:39:51Z shijia.wxr $
 */
package com.alibaba.rocketmq.common;

import java.util.concurrent.atomic.AtomicLong;


/**
 * 用来标识数据的版本号
 * 
 * @author vintage.wang@gmail.com shijia.wxr@taobao.com
 * 
 */
public class DataVersion {
    private long timestatmp = System.currentTimeMillis();
    private AtomicLong counter = new AtomicLong(0);


    public String encode() {
        return this.timestatmp + " " + this.counter.get();
    }


    public static DataVersion decode(final String data) {
        String[] strs = data.split(" ");
        DataVersion dv = new DataVersion();
        dv.timestatmp = Long.parseLong(strs[0]);
        dv.counter.set(Long.parseLong(strs[1]));
        return dv;
    }


    public String nextVersion() {
        this.counter.incrementAndGet();
        return this.encode();
    }


    public String currentVersion() {
        return this.encode();
    }


    @Override
    public boolean equals(Object obj) {
        DataVersion dv = (DataVersion) obj;
        return this.timestatmp == dv.timestatmp && this.counter.get() == dv.counter.get();
    }
}
