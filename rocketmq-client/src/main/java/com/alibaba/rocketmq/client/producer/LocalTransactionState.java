/**
 * 
 */
package com.alibaba.rocketmq.client.producer;

/**
 * Producer本地事务执行状态
 * 
 * @author vintage.wang@gmail.com shijia.wxr@taobao.com
 */
public enum LocalTransactionState {
    COMMIT_MESSAGE,
    ROLLBACK_MESSAGE,
    UNKNOW,
}
