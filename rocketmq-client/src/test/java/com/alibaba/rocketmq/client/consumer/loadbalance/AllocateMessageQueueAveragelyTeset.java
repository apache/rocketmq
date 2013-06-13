/**
 * $Id: AllocateMessageQueueAveragelyTeset.java 1831 2013-05-16 01:39:51Z shijia.wxr $
 */
package com.alibaba.rocketmq.client.consumer.loadbalance;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import com.alibaba.rocketmq.client.consumer.rebalance.AllocateMessageQueueAveragely;
import com.alibaba.rocketmq.common.message.MessageQueue;


public class AllocateMessageQueueAveragelyTeset {

    @Test
    public void test_allocate() {
        AllocateMessageQueueAveragely allocateMessageQueueAveragely = new AllocateMessageQueueAveragely();
        String topic = "topic_test";
        String currentCID = "CID";
        List<MessageQueue> mqAll = new ArrayList<MessageQueue>();
        for (int i = 0; i < 10; i++) {
            MessageQueue mq = new MessageQueue(topic, "brokerName", i);
            mqAll.add(mq);
        }

        List<String> cidAll = new ArrayList<String>();
        for (int j = 0; j < 3; j++) {
            cidAll.add("CID" + j);
        }
        System.out.println(mqAll.toString());
        System.out.println(cidAll.toString());
        for (int i = 0; i < 3; i++) {
            List<MessageQueue> rs = allocateMessageQueueAveragely.allocate(currentCID + i, mqAll, cidAll);
            System.out.println("rs[" + currentCID + i + "]:" + rs.toString());
        }

    }
}
