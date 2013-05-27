/**
 * $Id: TransactionMQProducer.java 1831 2013-05-16 01:39:51Z shijia.wxr $
 */
package com.alibaba.rocketmq.client.producer;

import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.common.Message;


/**
 * 支持分布式事务Producer
 * 
 * @author shijia.wxr<vintage.wang@gmail.com>
 * 
 */
public class TransactionMQProducer extends DefaultMQProducer {
    private TransactionCheckListener transactionCheckListener;
    private int checkThreadPoolMinSize = 1;
    private int checkThreadPoolMaxSize = 1;
    private int checkRequestHoldMax = 2000;


    public TransactionMQProducer() {
    }


    public TransactionMQProducer(final String producerGroup) {
        super(producerGroup);
    }


    @Override
    public void start() throws MQClientException {
        this.defaultMQProducerImpl.initTransactionEnv();
        super.start();
    }


    @Override
    public void shutdown() {
        super.shutdown();
        this.defaultMQProducerImpl.destroyTransactionEnv();
    }


    public SendResult sendMessageInTransaction(final Message msg, final LocalTransactionExecuter tranExecuter)
            throws MQClientException {
        if (null == this.transactionCheckListener) {
            throw new MQClientException("localTransactionBranchCheckListener is null", null);
        }

        return this.defaultMQProducerImpl.sendMessageInTransaction(msg, tranExecuter);
    }


    public TransactionCheckListener getTransactionCheckListener() {
        return transactionCheckListener;
    }


    public void setTransactionCheckListener(TransactionCheckListener transactionCheckListener) {
        this.transactionCheckListener = transactionCheckListener;
    }


    public int getCheckThreadPoolMinSize() {
        return checkThreadPoolMinSize;
    }


    public void setCheckThreadPoolMinSize(int checkThreadPoolMinSize) {
        this.checkThreadPoolMinSize = checkThreadPoolMinSize;
    }


    public int getCheckThreadPoolMaxSize() {
        return checkThreadPoolMaxSize;
    }


    public void setCheckThreadPoolMaxSize(int checkThreadPoolMaxSize) {
        this.checkThreadPoolMaxSize = checkThreadPoolMaxSize;
    }


    public int getCheckRequestHoldMax() {
        return checkRequestHoldMax;
    }


    public void setCheckRequestHoldMax(int checkRequestHoldMax) {
        this.checkRequestHoldMax = checkRequestHoldMax;
    }
}
