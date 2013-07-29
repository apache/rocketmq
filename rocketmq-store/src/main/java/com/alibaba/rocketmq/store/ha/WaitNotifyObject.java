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
package com.alibaba.rocketmq.store.ha;

import java.util.HashMap;


/**
 * 用来做线程之间异步通知
 * 
 * @author shijia.wxr<vintage.wang@gmail.com>
 * @since 2013-7-21
 */
public class WaitNotifyObject {
    // 是否已经被Notify过，广播模式
    protected final HashMap<Long/* thread id */, Boolean/* notified */> waitingThreadTable =
            new HashMap<Long, Boolean>(16);
    // 是否已经被Notify过
    protected volatile boolean hasNotified = false;


    public void wakeup() {
        synchronized (this) {
            if (!this.hasNotified) {
                this.hasNotified = true;
                this.notify();
            }
        }
    }


    protected void waitForRunning(long interval) {
        synchronized (this) {
            if (this.hasNotified) {
                this.hasNotified = false;
                this.onWaitEnd();
                return;
            }

            try {
                this.wait(interval);
            }
            catch (InterruptedException e) {
                e.printStackTrace();
            }
            finally {
                this.hasNotified = false;
                this.onWaitEnd();
            }
        }
    }


    protected void onWaitEnd() {
    }


    /**
     * 广播方式唤醒
     */
    public void wakeupAll() {
        synchronized (this) {
            boolean needNotify = false;

            for (Boolean value : this.waitingThreadTable.values()) {
                needNotify = needNotify || !value;
                value = true;
            }

            if (needNotify) {
                this.notifyAll();
            }
        }
    }


    /**
     * 多个线程调用wait
     */
    public void allWaitForRunning(long interval) {
        long currentThreadId = Thread.currentThread().getId();
        synchronized (this) {
            Boolean notified = this.waitingThreadTable.get(currentThreadId);
            if (notified != null && notified) {
                this.waitingThreadTable.put(currentThreadId, false);
                this.onWaitEnd();
                return;
            }

            try {
                this.wait(interval);
            }
            catch (InterruptedException e) {
                e.printStackTrace();
            }
            finally {
                this.waitingThreadTable.put(currentThreadId, false);
                this.onWaitEnd();
            }
        }
    }
}
