package com.alibaba.rocketmq.example.transaction;

import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.client.producer.DefaultMQProducer;
import com.alibaba.rocketmq.client.producer.LocalTransactionState;
import com.alibaba.rocketmq.client.producer.SendResult;
import com.alibaba.rocketmq.client.producer.SendStatus;
import com.alibaba.rocketmq.common.message.Message;
import com.alibaba.rocketmq.common.message.MessageAccessor;
import com.alibaba.rocketmq.common.message.MessageConst;

/**
 * Created by zhongliang.czl on 2015/10/22.
 */
public class EncoderSendResultTest {
    public static void main (String []args) throws MQClientException {
        DefaultMQProducer producer=new DefaultMQProducer("PID_MingduanTest");
        producer.start();
        producer.setNamesrvAddr("127.0.0.1:9876");
        try{
            Message msg=new Message("MingduanTest","tag1","keya","helloSend".getBytes());
            MessageAccessor.putProperty(msg, MessageConst.PROPERTY_TRANSACTION_PREPARED,"true");
            MessageAccessor.putProperty(msg, MessageConst.PROPERTY_PRODUCER_GROUP, producer.getProducerGroup());
            SendResult sendResult =producer.send(msg);
            String jsonstring = SendResult.encoderSendResultToJson(sendResult);
            System.out.println("the result string:  "+jsonstring);
            SendResult sendResult1=SendResult.decoderSendResultfromJson(jsonstring);
            System.out.println("the decode object:  "+sendResult1);
            if(sendResult.getSendStatus()== SendStatus.SEND_OK){
                producer.getDefaultMQProducerImpl().endTransaction
                        (sendResult1, LocalTransactionState.COMMIT_MESSAGE,null);
            }
        }catch (Exception e){
            e.printStackTrace();
        }


    }
}
