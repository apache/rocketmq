/**
 * $Id: PullMessageService.java 1831 2013-05-16 01:39:51Z shijia.wxr $
 */
package com.alibaba.rocketmq.client.impl.consumer;

import java.util.concurrent.LinkedBlockingQueue;

import com.alibaba.rocketmq.client.impl.factory.MQClientFactory;
import com.alibaba.rocketmq.common.ServiceThread;


/**
 * 长轮询拉消息服务，单线程异步拉取
 * 
 * @author shijia.wxr<vintage.wang@gmail.com>
 * 
 */
public class PullMessageService extends ServiceThread {
    private final LinkedBlockingQueue<PullRequest> pullRequestQueue = new LinkedBlockingQueue<PullRequest>();
    private final MQClientFactory mQClientFactory;


    public PullMessageService(MQClientFactory mQClientFactory) {
        this.mQClientFactory = mQClientFactory;
    }


    @Override
    public void run() {
        while (!this.isStoped()) {
            try {
                PullRequest pullRequest = this.pullRequestQueue.take();
            }
            catch (InterruptedException e) {
                // TODO log
            }
        }
    }


    @Override
    public String getServiceName() {
        return PullMessageService.class.getSimpleName();
    }
}
