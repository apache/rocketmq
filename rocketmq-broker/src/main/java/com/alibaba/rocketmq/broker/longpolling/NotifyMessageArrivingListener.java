package com.alibaba.rocketmq.broker.longpolling;


import com.alibaba.rocketmq.store.MessageArrivingListener;

/**
 * Created by manhong.yqd<jodie.yqd@gmail.com> on 15/6/19.
 */
public class NotifyMessageArrivingListener implements MessageArrivingListener {
    private final PullRequestHoldService pullRequestHoldService;


    public NotifyMessageArrivingListener(final PullRequestHoldService pullRequestHoldService) {
        this.pullRequestHoldService = pullRequestHoldService;
    }


    @Override
    public void arriving(String topic, int queueId, long logicOffset) {
        this.pullRequestHoldService.notifyMessageArriving(topic, queueId, logicOffset);
    }
}
