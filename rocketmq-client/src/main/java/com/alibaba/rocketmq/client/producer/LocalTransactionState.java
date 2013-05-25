/**
 * 
 */
package com.alibaba.rocketmq.client.producer;

/**
 * Producer本地事务执行状态
 * 
 * @author shijia.wxr<vintage.wang@gmail.com>
 */
public enum LocalTransactionState {
    COMMIT_MESSAGE,
    ROLLBACK_MESSAGE,
    UNKNOW,
}
