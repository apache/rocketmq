package org.apache.rocketmq.common.utils;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.annotation.concurrent.NotThreadSafe;

@NotThreadSafe
public class AsyncShutdownHelper {
    private final AtomicBoolean shutdown;
    private final List<Shutdown> targetList;

    private CountDownLatch countDownLatch;

    public AsyncShutdownHelper() {
        this.targetList = new ArrayList<>();
        this.shutdown = new AtomicBoolean(false);
    }

    public void addTarget(Shutdown target) {
        if (shutdown.get()) {
            return;
        }
        targetList.add(target);
    }

    public AsyncShutdownHelper shutdown() {
        if (shutdown.get()) {
            return this;
        }
        if (targetList.isEmpty()) {
            return this;
        }
        this.countDownLatch = new CountDownLatch(targetList.size());
        for (Shutdown target : targetList) {
            Runnable runnable = () -> {
                try {
                    target.shutdown();
                } catch (Exception ignored) {

                } finally {
                    countDownLatch.countDown();
                }
            };
            new Thread(runnable).start();
        }
        return this;
    }

    public boolean await(long time, TimeUnit unit) throws InterruptedException {
        if (shutdown.get()) {
            return false;
        }
        try {
            return this.countDownLatch.await(time, unit);
        } finally {
            shutdown.compareAndSet(false, true);
        }
    }
}
