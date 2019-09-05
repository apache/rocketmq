package org.apache.rocketmq.ekfet;

import org.apache.rocketmq.client.consumer.DefaultMQPullConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.List;

public class ConsumerTest {
   private  static DefaultMQPullConsumer consumer=new DefaultMQPullConsumer("test_group");

   static {
       consumer.setNamesrvAddr("129.0.0.1:7986");
       consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
       consumer.subscribe("TopicTest", "*");

       /*
        *  Register callback to execute on arrival of messages fetched from brokers.
        */
       consumer.registerMessageListener(new MessageListenerConcurrently() {

           @Override
           public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs,
                                                           ConsumeConcurrentlyContext context) {
               System.out.printf("%s Receive New Messages: %s %n", Thread.currentThread().getName(), msgs);
               return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
           }
       });

       /*
        *  Launch the consumer instance.
        */
       consumer.start();

       System.out.printf("Consumer Started.%n");
   }

    public static void main(String[] args) {
        defaultMQPullConsumer.start();

    }
}
