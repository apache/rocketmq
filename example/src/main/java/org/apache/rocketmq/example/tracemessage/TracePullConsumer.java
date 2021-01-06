package org.apache.rocketmq.example.tracemessage;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.rocketmq.client.consumer.DefaultLitePullConsumer;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;

public class TracePullConsumer {

    public static volatile boolean running = true;

    public static void main(String[] args) throws Exception {

        DefaultLitePullConsumer litePullConsumer = new DefaultLitePullConsumer("cid-pz2",true);
        litePullConsumer.setAutoCommit(false);
        litePullConsumer.start();
        Collection<MessageQueue> mqSet = litePullConsumer.fetchMessageQueues("pzTopicTest");
        List<MessageQueue> list = new ArrayList<>(mqSet);
        List<MessageQueue> assignList = new ArrayList<>();
        for (int i = 0; i < list.size() / 2; i++) {
            assignList.add(list.get(i));
        }
        litePullConsumer.assign(assignList);
        litePullConsumer.seek(assignList.get(0), 2);
        try {
            while (running) {
                List<MessageExt> messageExts = litePullConsumer.poll();
                System.out.printf("%s %n", messageExts);
                litePullConsumer.commitSync();
            }
        } finally {
            litePullConsumer.shutdown();
        }

    }

}
