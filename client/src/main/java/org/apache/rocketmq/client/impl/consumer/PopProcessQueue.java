/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.client.impl.consumer;

import java.util.concurrent.atomic.AtomicInteger;
import org.apache.rocketmq.remoting.protocol.body.PopProcessQueueInfo;

/**
 * Queue consumption snapshot
 */
public class PopProcessQueue {

    private final static long PULL_MAX_IDLE_TIME = Long.parseLong(System.getProperty("rocketmq.client.pull.pullMaxIdleTime", "120000"));

    private long lastPopTimestamp = System.currentTimeMillis();
    private AtomicInteger waitAckCounter = new AtomicInteger(0);
    private volatile boolean dropped = false;

    public long getLastPopTimestamp() {
        return lastPopTimestamp;
    }

    public void setLastPopTimestamp(long lastPopTimestamp) {
        this.lastPopTimestamp = lastPopTimestamp;
    }

    public void incFoundMsg(int count) {
        this.waitAckCounter.getAndAdd(count);
    }

    /**
     * @return the value before decrement.
     */
    public int ack() {
        return this.waitAckCounter.getAndDecrement();
    }

    public void decFoundMsg(int count) {
        this.waitAckCounter.addAndGet(count);
    }

    public int getWaiAckMsgCount() {
        return this.waitAckCounter.get();
    }

    public boolean isDropped() {
        return dropped;
    }

    public void setDropped(boolean dropped) {
        this.dropped = dropped;
    }

    public void fillPopProcessQueueInfo(final PopProcessQueueInfo info) {
        info.setWaitAckCount(getWaiAckMsgCount());
        info.setDropped(isDropped());
        info.setLastPopTimestamp(getLastPopTimestamp());
    }

    public boolean isPullExpired() {
        return (System.currentTimeMillis() - this.lastPopTimestamp) > PULL_MAX_IDLE_TIME;
    }

    @Override
    public String toString() {
        return "PopProcessQueue[waitAckCounter:" + this.waitAckCounter.get()
                + ", lastPopTimestamp:" + getLastPopTimestamp()
                + ", drop:" + dropped +  "]";
    }
}
