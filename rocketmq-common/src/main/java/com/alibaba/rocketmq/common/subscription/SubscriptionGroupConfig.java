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
    // 重试消费最大次数，超过则投递到死信队列，不再投递，并报警
    private int retryMaxTimes = 8;
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


    public int getRetryMaxTimes() {
        return retryMaxTimes;
    }


    public void setRetryMaxTimes(int retryMaxTimes) {
        this.retryMaxTimes = retryMaxTimes;
    }


    public long getBrokerId() {
        return brokerId;
    }


    public void setBrokerId(long brokerId) {
        this.brokerId = brokerId;
    }


    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (int) (brokerId ^ (brokerId >>> 32));
        result = prime * result + (consumeEnable ? 1231 : 1237);
        result = prime * result + ((groupName == null) ? 0 : groupName.hashCode());
        result = prime * result + retryMaxTimes;
        result = prime * result + retryQueueNums;
        return result;
    }


    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        SubscriptionGroupConfig other = (SubscriptionGroupConfig) obj;
        if (brokerId != other.brokerId)
            return false;
        if (consumeEnable != other.consumeEnable)
            return false;
        if (groupName == null) {
            if (other.groupName != null)
                return false;
        }
        else if (!groupName.equals(other.groupName))
            return false;
        if (retryMaxTimes != other.retryMaxTimes)
            return false;
        if (retryQueueNums != other.retryQueueNums)
            return false;
        return true;
    }


    @Override
    public String toString() {
        return "SubscriptionGroupConfig [groupName=" + groupName + ", consumeEnable=" + consumeEnable
                + ", retryQueueNums=" + retryQueueNums + ", retryMaxTimes=" + retryMaxTimes + ", brokerId="
                + brokerId + "]";
    }
}
