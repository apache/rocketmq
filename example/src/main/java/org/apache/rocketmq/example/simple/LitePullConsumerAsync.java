package org.apache.rocketmq.example.simple;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.rocketmq.client.consumer.DefaultLitePullConsumer;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;

public class LitePullConsumerAsync {
    public static volatile boolean running = true;

    public static void main(String[] args) throws Exception {
        DefaultLitePullConsumer litePullConsumer = new DefaultLitePullConsumer("please_rename_unique_group_name");
        litePullConsumer.setNamesrvAddr("127.0.0.1:9876");
        litePullConsumer.setAutoCommit(false);
        litePullConsumer.start();

        Collection<MessageQueue> mqSet = litePullConsumer.fetchMessageQueues("TopicTest");
        List<MessageQueue> list = new ArrayList<>(mqSet);
        System.out.println(list.size());

        litePullConsumer.assign(list);
        litePullConsumer.seek(list.get(0), 0);
        try {
            while (running) {
                List<MessageExt> messageExts = litePullConsumer.poll();
                for(MessageExt me : messageExts) {
                    // messages in different messageQueue will be Async consumed
                    // You can observe the QueueId and QueueOffset of messages
                    System.out.println("------------------");
                    System.out.printf("msg id: %s %n", me.getMsgId());
                    System.out.printf("QueueId: %s %n", me.getQueueId());
                    System.out.printf("QueueOffset: %s %n", me.getQueueOffset());
                }
                litePullConsumer.commit();
            }
        } finally {
            litePullConsumer.shutdown();
        }
    }
}
