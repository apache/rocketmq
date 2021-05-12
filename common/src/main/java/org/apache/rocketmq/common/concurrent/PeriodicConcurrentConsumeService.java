package org.apache.rocketmq.common.concurrent;

import org.apache.rocketmq.common.ServiceThread;

/**
 * @author zhangzicheng
 * @date 2021/05/12
 */
public class PeriodicConcurrentConsumeService extends ServiceThread {

    @Override
    public void run() {
        while (!this.isStopped()) {
            PriorityConcurrentEngine.invokeAllNow();
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
