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
package org.apache.rocketmq.common;

import org.apache.rocketmq.common.constant.LoggerName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 服务线程
 */
public abstract class ServiceThread implements Runnable {
    private static final Logger STLOG = LoggerFactory.getLogger(LoggerName.COMMON_LOGGER_NAME);
    private static final long JOIN_TIME = 90 * 1000;

    /**
     * 线程
     */
    protected final Thread thread;
    /**
     * 等待标识
     */
    protected final CountDownLatch2 waitPoint = new CountDownLatch2(1);
    // TODO 疑问：这个变量的用途没看懂？
    protected volatile AtomicBoolean hasNotified = new AtomicBoolean(false);
    /**
     * 是否停止
     */
    protected volatile boolean stopped = false;

    public ServiceThread() {
        this.thread = new Thread(this, this.getServiceName());
    }

    public abstract String getServiceName();

    public void start() {
        this.thread.start();
    }

    public void shutdown() {
        this.shutdown(false);
    }

    public void shutdown(final boolean interrupt) {
        this.stopped = true;
        STLOG.info("shutdown thread " + this.getServiceName() + " interrupt " + interrupt);

        if (hasNotified.compareAndSet(false, true)) {
            waitPoint.countDown(); // notify
        }

        try {
            if (interrupt) {
                this.thread.interrupt();
            }

            long beginTime = System.currentTimeMillis();
            if (!this.thread.isDaemon()) {
                this.thread.join(this.getJointime());
            }
            long eclipseTime = System.currentTimeMillis() - beginTime;
            STLOG.info("join thread " + this.getServiceName() + " eclipse time(ms) " + eclipseTime + " "
                + this.getJointime());
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public long getJointime() {
        return JOIN_TIME;
    }

    public void stop() {
        this.stop(false);
    }

    public void stop(final boolean interrupt) {
        this.stopped = true;
        STLOG.info("stop thread " + this.getServiceName() + " interrupt " + interrupt);

        if (hasNotified.compareAndSet(false, true)) {
            waitPoint.countDown(); // notify
        }

        if (interrupt) {
            this.thread.interrupt();
        }
    }

    public void makeStop() {
        this.stopped = true;
        STLOG.info("makestop thread " + this.getServiceName());
    }

    /**
     * 唤醒
     */
    @SuppressWarnings("SpellCheckingInspection")
    public void wakeup() {
        if (hasNotified.compareAndSet(false, true)) {
            waitPoint.countDown(); // notify
        }
    }

    /**
     * 等待 interval毫秒 运行
     * 如果 {@link #hasNotified} 为 true 时，等待直接结束。
     *
     * @param interval 等待市场
     */
    protected void waitForRunning(long interval) {
        if (hasNotified.compareAndSet(true, false)) {
            this.onWaitEnd();
            return;
        }

        //entry to wait
        waitPoint.reset();

        try {
            waitPoint.await(interval, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            hasNotified.set(false);
            this.onWaitEnd();
        }
    }

    protected void onWaitEnd() {
    }

    public boolean isStopped() {
        return stopped;
    }
}
