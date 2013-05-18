/**
 * 
 */
package com.alibaba.rocketmq.store.transaction;

/**
 * 存储层向Producer回查事务状态
 * 
 * @author vintage.wang@gmail.com shijia.wxr@taobao.com
 */
public interface TransactionCheckExecuter {
    public void gotoCheck(//
            final int producerGroupHashCode,//
            final long tranStateTableOffset,//
            final long commitLogOffset,//
            final int msgSize);
}
