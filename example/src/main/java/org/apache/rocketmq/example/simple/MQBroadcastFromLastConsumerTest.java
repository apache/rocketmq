package org.apache.rocketmq.example.simple;

import org.apache.rocketmq.client.consumer.*;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.common.RemotingHelper;

import java.util.List;

/**
 * Created by l_yy on 2017/11/2.
 */
public class MQBroadcastFromLastConsumerTest {



    public static void main(String[] args) throws MQClientException {


        final MQBroadcastFromLastConsumer scheduleService = new MQBroadcastFromLastConsumer("test_topic_group", 20);

        scheduleService.setNamesrvAddr("127.0.0.1:9876");
        scheduleService.setInstanceName("RISK_CONTROL_CORE_CONSUMER");

        scheduleService.registerPullTaskCallback("test_topic", new PullTaskCallback() {
            @Override
            public void doPullTask(MessageQueue mq, PullTaskContext context) {
                DefaultMQPullConsumer consumer = (DefaultMQPullConsumer)context.getPullConsumer();
                try {
                    long offset = consumer.fetchConsumeOffset(mq, false);
                    if (offset < 0)
                        offset = 0;

                    PullResult pullResult = consumer.pull(mq, "*", offset, 32);
                    if (pullResult == null) {
                        return;
                    }

                    switch (pullResult.getPullStatus()) {
                        case FOUND:
                            doFound(pullResult);
                        case NO_MATCHED_MSG:
                            context.setPullNextDelayTimeMillis(100);
                            break;
                        case NO_NEW_MSG:
                            context.setPullNextDelayTimeMillis(100);
                            break;
                        case OFFSET_ILLEGAL:
                            context.setPullNextDelayTimeMillis(100);
                            break;
                        default:
                            context.setPullNextDelayTimeMillis(100);
                            break;
                    }

                    consumer.updateConsumeOffset(mq, pullResult.getNextBeginOffset());
                } catch (Exception e) {

                }
            }
        });

        scheduleService.start();

    }


    public static void doFound(PullResult pullResult) {
        List<MessageExt> msgFoundList= pullResult.getMsgFoundList();

        for (MessageExt messageExt : msgFoundList) {
            try {
                String content = new String(messageExt.getBody(), RemotingHelper.DEFAULT_CHARSET);

                System.out.println(content);
            } catch (Exception e) {
            }
        }
    }

}
