/**
 * Copyright (C) 2010-2013 Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.rocketmq.common;

import java.util.concurrent.atomic.AtomicLong;

import com.alibaba.rocketmq.remoting.protocol.RemotingSerializable;


/**
 * 用来标识数据的版本号
 * 
 * @author shijia.wxr<vintage.wang@gmail.com>
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
