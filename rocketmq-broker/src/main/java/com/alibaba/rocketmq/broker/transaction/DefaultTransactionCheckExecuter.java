package com.alibaba.rocketmq.broker.transaction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.rocketmq.broker.BrokerController;
import com.alibaba.rocketmq.common.MixAll;
import com.alibaba.rocketmq.store.transaction.TransactionCheckExecuter;


/**
 * 存储层回调此接口，用来主动回查Producer的事务状态
 * 
 * @author vintage.wang@gmail.com shijia.wxr@taobao.com
 */
public class DefaultTransactionCheckExecuter implements TransactionCheckExecuter {
    private static final Logger log = LoggerFactory.getLogger(MixAll.TransactionLoggerName);
    private final BrokerController brokerController;


    public DefaultTransactionCheckExecuter(final BrokerController brokerController) {
        this.brokerController = brokerController;
    }


    @Override
    public void gotoCheck(int producerGroupHashCode, long tranStateTableOffset, long commitLogOffset, int msgSize) {
        // 第一步、查询Producer
        // 第二步、查询消息
        // 第三步、向Producer发起异步RPC请求
        // 第四步、收到异步应答后，开始处理应答结果
    }
}
