package org.apache.rocketmq.common.concurrent;

import org.apache.rocketmq.common.ServiceThread;

/**
 * @author ZhangZiCheng
 * @date 2021/05/12
 */
public class PeriodicConcurrentConsumeService extends ServiceThread {

    @Override
    public void run() {
        while (!this.isStopped()) {
            try {
                Thread.sleep(1);
                PriorityConcurrentEngine.invokeAllNow();
            } catch (InterruptedException ignored) {
            }
        }
    }

    @Override
    public String getServiceName() {
        return PeriodicConcurrentConsumeService.class.getSimpleName();
    }

    @Override
    public long getJointime() {
        return 0;
    }
}
