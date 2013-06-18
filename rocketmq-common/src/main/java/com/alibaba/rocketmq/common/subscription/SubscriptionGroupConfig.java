package com.alibaba.rocketmq.common.subscription;

import com.alibaba.rocketmq.common.MixAll;


/**
 * @author shijia.wxr<vintage.wang@gmail.com>
 * @since 2013-6-18
 */
public class SubscriptionGroupConfig {
    // 订阅组名
    private String groupName;
    // 消费功能是否开启
    private boolean consumeEnable = true;
    // 消费失败的消息放到一个重试队列，每个订阅组配置几个重试队列
    private int retryQueueNums = 1;
    // 从哪个Broker开始消费
    private long brokerId = MixAll.MASTER_ID;


    public String getGroupName() {
        return groupName;
    }


    public void setGroupName(String groupName) {
        this.groupName = groupName;
    }


    public boolean isConsumeEnable() {
        return consumeEnable;
    }


    public void setConsumeEnable(boolean consumeEnable) {
        this.consumeEnable = consumeEnable;
    }


    public int getRetryQueueNums() {
        return retryQueueNums;
    }


    public void setRetryQueueNums(int retryQueueNums) {
        this.retryQueueNums = retryQueueNums;
    }


    public long getBrokerId() {
        return brokerId;
    }


    public void setBrokerId(long brokerId) {
        this.brokerId = brokerId;
    }


    @Override
    public String toString() {
        return "SubscriptionGroupConfig [groupName=" + groupName + ", consumeEnable=" + consumeEnable
                + ", retryQueueNums=" + retryQueueNums + ", brokerId=" + brokerId + "]";
    }
}
