/**
 * $Id: PullMessageService.java 1831 2013-05-16 01:39:51Z shijia.wxr $
 */
package com.alibaba.rocketmq.client.impl.consumer;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;

import org.slf4j.Logger;

import com.alibaba.rocketmq.client.impl.factory.MQClientFactory;
import com.alibaba.rocketmq.client.log.ClientLogger;
import com.alibaba.rocketmq.common.ServiceThread;


/**
 * 长轮询拉消息服务，单线程异步拉取
 * 
 * @author shijia.wxr<vintage.wang@gmail.com>
 */
public class PullMessageService extends ServiceThread {
    private final Logger log = ClientLogger.getLog();
    private final LinkedBlockingQueue<PullRequest> pullRequestQueue = new LinkedBlockingQueue<PullRequest>();
    private final MQClientFactory mQClientFactory;
    // 与Factory对象共用一个定时对象
    private final ScheduledExecutorService scheduledExecutorService;


    public PullMessageService(MQClientFactory mQClientFactory) {
        this.mQClientFactory = mQClientFactory;
        this.scheduledExecutorService = mQClientFactory.getScheduledExecutorService();
    }


    private void pullMessage(final PullRequest pullRequest) {
        final MQConsumerInner consumer = this.mQClientFactory.selectConsumer(pullRequest.getConsumerGroup());
        if (consumer != null) {
            DefaultMQPushConsumerImpl impl = (DefaultMQPushConsumerImpl) consumer;
            impl.pullMessage(pullRequest);
        }
        else {
            // TODO log
        }
    }


    @Override
    public void run() {
        while (!this.isStoped()) {
            try {
                PullRequest pullRequest = this.pullRequestQueue.take();
                if (pullRequest != null) {
                    this.pullMessage(pullRequest);
                }
            }
            catch (Exception e) {
                // TODO log
            }
        }
    }


    @Override
    public String getServiceName() {
        return PullMessageService.class.getSimpleName();
    }
}
