package org.apache.rocketmq.example.schdule;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.Message;

public class StartDeliverTimeProducer {
    // modify broker config messageDelayLevel=1s 2s 3s 4s 5s
    public static void main(String[] args) throws Throwable{
        DefaultMQProducer producer = new DefaultMQProducer("StartDeliverTimeProducer");
        producer.setNamesrvAddr("localhost:9876");
        producer.start();
        for (int i = 0; i < 1000; i++) {
            long startDeliverTime = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(10L + ThreadLocalRandom.current().nextLong(10L));
            Message msg = new Message("TopicTest", (startDeliverTime + "").getBytes());
            msg.setStartDeliverTime(startDeliverTime);
            producer.send(msg);
        }
        producer.shutdown();
    }
}
