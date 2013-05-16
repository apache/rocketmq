/**
 * $Id: TransactionMQProducer.java 1831 2013-05-16 01:39:51Z shijia.wxr $
 */
package com.alibaba.rocketmq.client.producer;

import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.common.Message;


/**
 * 支持分布式事务Producer
 * 
 * @author vintage.wang@gmail.com shijia.wxr@taobao.com
 * 
 */
public class TransactionMQProducer extends DefaultMQProducer {
    private TransactionCheckListener transactionCheckListener;


    public TransactionMQProducer() {
    }


    public TransactionMQProducer(final String producerGroup) {
        super(producerGroup);
    }


    public void sendMessageInTransaction(final Message msg, final LocalTransactionExecuter tranExecuter)
            throws MQClientException {
        if (null == this.transactionCheckListener) {
            throw new MQClientException("localTransactionBranchCheckListener is null", null);
        }

        this.defaultMQProducerImpl.sendMessageInTransaction(msg, tranExecuter);
    }


    public TransactionCheckListener getTransactionCheckListener() {
        return transactionCheckListener;
    }


    public void setTransactionCheckListener(TransactionCheckListener transactionCheckListener) {
        this.transactionCheckListener = transactionCheckListener;
    }
}
