/**
 * $Id: Producer.java 1831 2013-05-16 01:39:51Z shijia.wxr $
 */
package com.alibaba.rocketmq.example.transaction;

import com.alibaba.rocketmq.client.exception.MQBrokerException;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.client.producer.DefaultMQProducer;
import com.alibaba.rocketmq.client.producer.LocalTransactionExecuter;
import com.alibaba.rocketmq.client.producer.MQProducer;
import com.alibaba.rocketmq.client.producer.SendResult;
import com.alibaba.rocketmq.client.producer.TransactionCheckListener;
import com.alibaba.rocketmq.client.producer.TransactionMQProducer;
import com.alibaba.rocketmq.common.Message;
import com.alibaba.rocketmq.remoting.exception.RemotingException;


/**
 * @author vintage.wang@gmail.com shijia.wxr@taobao.com
 * 
 */
public class TransactionProducer {
    public static void main(String[] args) throws MQClientException {
        
        	TransactionCheckListener transactionCheckListener = new TransactionCheckListenerImpl();
        	TransactionMQProducer producer = new TransactionMQProducer("example.producer");
        	producer.setTransactionCheckListener(transactionCheckListener);
            producer.start();

            String[] tags = new String[] { "TagA", "TagB", "TagC", "TagD", "TagE" };
            TranExecuterImpl tranExecuter = new TranExecuterImpl();
            for (int i = 0; i < 100; i++) {
            	try {
	                Message msg =
	                        new Message("TopicTest", tags[i % tags.length], "KEY" + i, ("Hello RocketMQ " + i).getBytes());
	                tranExecuter.setTransactionStats(1);
	                producer.sendMessageInTransaction(msg, tranExecuter);
            	}
                catch (MQClientException e) {
                    e.printStackTrace();
                }
            }

            producer.shutdown();
        
    }
}
