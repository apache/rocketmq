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
    // 是否允许从队列最小位置开始消费，线上默认会设置为false
    private boolean consumeFromMinEnable = true;
    // 是否允许广播方式消费
    private boolean consumeBroadcastEnable = true;
    // 消费失败的消息放到一个重试队列，每个订阅组配置几个重试队列
    private int retryQueueNums = 1;
    // 重试消费最大次数，超过则投递到死信队列，不再投递，并报警
    private int retryMaxTimes = 16;
    // 从哪个Broker开始消费
    private long brokerId = MixAll.MASTER_ID;
    // 发现消息堆积后，将Consumer的消费请求重定向到另外一台Slave机器
    private long whichBrokerWhenConsumeSlowly = 1;


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


    public boolean isConsumeFromMinEnable() {
        return consumeFromMinEnable;
    }


    public void setConsumeFromMinEnable(boolean consumeFromMinEnable) {
        this.consumeFromMinEnable = consumeFromMinEnable;
    }


    public boolean isConsumeBroadcastEnable() {
        return consumeBroadcastEnable;
    }


    public void setConsumeBroadcastEnable(boolean consumeBroadcastEnable) {
        this.consumeBroadcastEnable = consumeBroadcastEnable;
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


    public long getWhichBrokerWhenConsumeSlowly() {
        return whichBrokerWhenConsumeSlowly;
    }


    public void setWhichBrokerWhenConsumeSlowly(long whichBrokerWhenConsumeSlowly) {
        this.whichBrokerWhenConsumeSlowly = whichBrokerWhenConsumeSlowly;
    }


    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (int) (brokerId ^ (brokerId >>> 32));
        result = prime * result + (consumeBroadcastEnable ? 1231 : 1237);
        result = prime * result + (consumeEnable ? 1231 : 1237);
        result = prime * result + (consumeFromMinEnable ? 1231 : 1237);
        result = prime * result + ((groupName == null) ? 0 : groupName.hashCode());
        result = prime * result + retryMaxTimes;
        result = prime * result + retryQueueNums;
        result =
                prime * result + (int) (whichBrokerWhenConsumeSlowly ^ (whichBrokerWhenConsumeSlowly >>> 32));
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
        if (consumeBroadcastEnable != other.consumeBroadcastEnable)
            return false;
        if (consumeEnable != other.consumeEnable)
            return false;
        if (consumeFromMinEnable != other.consumeFromMinEnable)
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
        if (whichBrokerWhenConsumeSlowly != other.whichBrokerWhenConsumeSlowly)
            return false;
        return true;
    }


    @Override
    public String toString() {
        return "SubscriptionGroupConfig [groupName=" + groupName + ", consumeEnable=" + consumeEnable
                + ", consumeFromMinEnable=" + consumeFromMinEnable + ", consumeBroadcastEnable="
                + consumeBroadcastEnable + ", retryQueueNums=" + retryQueueNums + ", retryMaxTimes="
                + retryMaxTimes + ", brokerId=" + brokerId + ", whichBrokerWhenConsumeSlowly="
                + whichBrokerWhenConsumeSlowly + "]";
    }
}
