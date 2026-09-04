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

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;

import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

public abstract class ServiceThread implements Runnable {
    protected static final Logger log = LoggerFactory.getLogger(LoggerName.COMMON_LOGGER_NAME);

    private static final long JOIN_TIME = 90 * 1000;

    protected volatile Thread thread;
    protected final AtomicBoolean hasNotified = new AtomicBoolean(false);
    protected volatile boolean stopped = false;
    protected boolean isDaemon = false;

    //Make it able to restart the thread
    private final AtomicBoolean started = new AtomicBoolean(false);

    public ServiceThread() {

    }

    public String getServiceName() {
        return this.getClass().getSimpleName();
    }

    public void start() {
        log.info("Try to start service thread:{} started:{} lastThread:{}", getServiceName(), started.get(), thread);
        if (!started.compareAndSet(false, true)) {
            return;
        }
        stopped = false;
        this.thread = new Thread(this, getServiceName());
        this.thread.setDaemon(isDaemon);
        this.thread.start();
        log.info("Start service thread:{} started:{} lastThread:{}", getServiceName(), started.get(), thread);
    }

    public void shutdown() {
        this.shutdown(false);
    }

    public void shutdown(final boolean interrupt) {
        log.info("Try to shutdown service thread:{} started:{} lastThread:{}", getServiceName(), started.get(), thread);
        if (!started.compareAndSet(true, false)) {
            return;
        }
        this.stopped = true;
        log.info("shutdown thread[{}] interrupt={} ", getServiceName(), interrupt);

        //if thead is waiting, wakeup it
        wakeup();

        try {
            if (interrupt) {
                this.thread.interrupt();
            }

            long beginTime = System.currentTimeMillis();
            if (!this.thread.isDaemon()) {
                this.thread.join(this.getJoinTime());
            }
            long elapsedTime = System.currentTimeMillis() - beginTime;
            log.info("join thread[{}], elapsed time: {}ms, join time:{}ms", getServiceName(), elapsedTime, this.getJoinTime());
        } catch (InterruptedException e) {
            log.error("Interrupted", e);
        }
    }

    public long getJoinTime() {
        return JOIN_TIME;
    }

    public void makeStop() {
        if (!started.get()) {
            return;
        }
        this.stopped = true;
        // wake up the parked worker so it observes the stop flag promptly
        wakeup();
        log.info("makestop thread[{}] ", this.getServiceName());
    }

    public void wakeup() {
        if (hasNotified.get()) {
            return;
        }
        if (hasNotified.compareAndSet(false, true)) {
            LockSupport.unpark(this.thread); // notify
        }
    }

    protected void waitForRunning(long interval) {
        // Publish the parking thread so wakeup() can target it (also handles restart).
        this.thread = Thread.currentThread();

        if (hasNotified.compareAndSet(true, false)) {
            this.onWaitEnd();
            return;
        }

        // LockSupport permits are sticky: an unpark delivered before park makes the next park
        // return at once, and the loop re-checks hasNotified, so no wakeup can be lost.
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(interval);
        while (!hasNotified.get()) {
            if (stopped) {
                break;
            }
            long remain = deadline - System.nanoTime();
            if (remain <= 0) {
                break;
            }
            LockSupport.parkNanos(this, remain);
            if (Thread.interrupted()) {
                Thread.currentThread().interrupt();
                break;
            }
        }

        hasNotified.set(false);
        this.onWaitEnd();
    }

    protected void onWaitEnd() {
    }

    public boolean isStopped() {
        return stopped;
    }

    public boolean isDaemon() {
        return isDaemon;
    }

    public void setDaemon(boolean daemon) {
        isDaemon = daemon;
    }
}
