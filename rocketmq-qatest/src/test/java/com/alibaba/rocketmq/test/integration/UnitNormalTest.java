package com.alibaba.rocketmq.test.integration;

import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.client.producer.DefaultMQProducer;
import com.alibaba.rocketmq.client.producer.SendResult;
import com.alibaba.rocketmq.common.message.Message;

import java.util.concurrent.TimeUnit;


/**
 * description
 * 
 * @author: manhong.yqd<jodie.yqd@gmail.com>
 * @since: 14-3-19
 */
public class UnitNormalTest {
    private static String[] topics = new String[] { "qatest_TopicTest" };
    private static String[] nsAddrs = new String[] { "10.232.26.122:9876", "10.232.25.83:9876" };
    // private static String[] topics = new String[] {
    // "us_and_center_same_topic_send_to_center" };
    // private static String[] nsAddrs = new String[] { "10.125.5.208:9876",
    // "10.125.1.76:9876" };
    private static String[] tags = new String[] { "TagA", "TagB", "TagC", "TagD", "TagE" };
    private static String producerGroup = "qatest_producer";
    private static DaoService dao = new DaoService();


    public static void main(String[] args) throws MQClientException, InterruptedException {
        // 只发一个单元
        DefaultMQProducer producer0 = crateProducer(producerGroup, nsAddrs[1]);
        producer0.start();
        sendMsg(producer0, topics);
    }


    private static DefaultMQProducer crateProducer(String group, String nsAddr) throws MQClientException {
        DefaultMQProducer producer = new DefaultMQProducer(group);
        producer.setInstanceName("unit_producer_" + System.nanoTime());
        producer.setNamesrvAddr(nsAddr);
        return producer;
    }


    public static void sendMsg(DefaultMQProducer producer, String... topics) throws MQClientException,
            InterruptedException {
        for (int j = 0; j < 999999999; j++) {
            for (String topic : topics) {
                try {
                    String body = (topic + "#" + j);
                    Message msg = new Message(topic, tags[j % tags.length], "KEY" + j, body.getBytes());
                    SendResult sendResult = producer.send(msg);
                    System.out.println("sendStatus=" + sendResult.getSendStatus() + ", topic="
                            + msg.getTopic() + ", body=" + new String(msg.getBody()));
                    // dao.insert(body);
                }
                catch (Exception e) {
                    e.printStackTrace();
                    TimeUnit.SECONDS.sleep(1);
                }
            }
            // TimeUnit.SECONDS.sleep(1);
        }
    }
}
