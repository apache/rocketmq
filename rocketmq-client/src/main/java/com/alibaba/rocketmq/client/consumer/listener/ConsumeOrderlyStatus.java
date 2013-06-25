package com.alibaba.rocketmq.client.consumer.listener;

/**
 * 
 * @author shijia.wxr<vintage.wang@gmail.com>
 * 
 */
public enum ConsumeOrderlyStatus {
    // 消息处理成功
    SUCCESS,
    // 回滚消息
    ROLLBACK,
    // 提交消息
    COMMIT,
    // 将当前队列挂起一小会儿
    SUSPEND_CURRENT_QUEUE_A_MOMENT,
}
