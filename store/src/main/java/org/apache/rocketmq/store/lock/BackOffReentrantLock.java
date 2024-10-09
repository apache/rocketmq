package org.apache.rocketmq.store.lock;

import java.util.concurrent.locks.ReentrantLock;

public class BackOffReentrantLock implements AdaptiveBackOffSpinLock{
    private ReentrantLock putMessageNormalLock = new ReentrantLock(); // NonfairSync

    @Override
    public void lock() {
        putMessageNormalLock.lock();
    }

    @Override
    public void unlock() {
        putMessageNormalLock.unlock();
    }
}
