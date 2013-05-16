/**
 * $Id: ConsumeOrderlyStatus.java 1831 2013-05-16 01:39:51Z shijia.wxr $
 */
package com.alibaba.rocketmq.client.consumer.listener;

/**
 * 
 * @author vintage.wang@gmail.com shijia.wxr@taobao.com
 * 
 */
public enum ConsumeOrderlyStatus {
    // 消息处理成功
    SUCCESS,
    // 回滚消息
    ROLLBACK,
    // 提交消息
    COMMIT,
    // 立刻重试
    RETRY_IMMEDIATELY,
    // 将当前队列挂起一小会儿
    SUSPEND_CURRENT_QUEUE_A_MOMENT,
}
