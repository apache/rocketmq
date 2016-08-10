package com.alibaba.rocketmq.broker.latency;

import com.alibaba.rocketmq.broker.BrokerController;
import com.alibaba.rocketmq.common.ThreadFactoryImpl;
import com.alibaba.rocketmq.remoting.netty.RequestTask;
import com.alibaba.rocketmq.remoting.protocol.RemotingSysResponseCode;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;


/**
 * @author shijia.wxr
 */
public class BrokerFastFailure {
    private final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryImpl(
            "BrokerFastFailureScheduledThread"));
    private final BrokerController brokerController;

    public BrokerFastFailure(final BrokerController brokerController) {
        this.brokerController = brokerController;
    }

    public void start() {
        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                cleanExpiredRequest();
            }
        }, 1000, 10, TimeUnit.MILLISECONDS);
    }

    private void cleanExpiredRequest() {
        while (this.brokerController.getMessageStore().isOSPageCacheBusy()) {
            try {
                if (!this.brokerController.getSendThreadPoolQueue().isEmpty()) {
                    final Runnable runnable = this.brokerController.getSendThreadPoolQueue().poll(0, TimeUnit.SECONDS);
                    if (null == runnable) {
                        break;
                    }
                    final RequestTask rt = (RequestTask) runnable;
                    rt.returnResponse(RemotingSysResponseCode.SYSTEM_BUSY, String.format("[PC_CLEAN_QUEUE]broker busy, start flow control for a while, period in queue: %sms", System.currentTimeMillis() - rt.getCreateTimestamp()));
                } else {
                    break;
                }
            } catch (Exception e) {
            }
        }

        while (true) {
            try {
                if (!this.brokerController.getSendThreadPoolQueue().isEmpty()) {
                    final Runnable runnable = this.brokerController.getSendThreadPoolQueue().peek();
                    if (null == runnable) {
                        break;
                    }
                    final RequestTask rt = (RequestTask) runnable;
                    if (rt.isStopRun()) {
                        break;
                    }

                    final long behind = System.currentTimeMillis() - rt.getCreateTimestamp();
                    if (behind >= this.brokerController.getBrokerConfig().getWaitTimeMillsInSendQueue()) {
                        this.brokerController.getSendThreadPoolQueue().remove(runnable);
                        rt.setStopRun(true);
                        rt.returnResponse(RemotingSysResponseCode.SYSTEM_BUSY, String.format("[TIMEOUT_CLEAN_QUEUE]broker busy, start flow control for a while, period in queue: %sms", behind));
                    } else {
                        break;
                    }
                } else {
                    break;
                }
            } catch (Exception e) {
            }
        }
    }

    public void shutdown() {
        this.scheduledExecutorService.shutdown();
    }
}
