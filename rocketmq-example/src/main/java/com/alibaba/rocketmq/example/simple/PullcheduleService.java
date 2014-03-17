package com.alibaba.rocketmq.example.simple;

import com.alibaba.rocketmq.client.consumer.DefaultMQPullConsumer;
import com.alibaba.rocketmq.client.consumer.MQPullConsumerScheduleService;
import com.alibaba.rocketmq.client.consumer.PullResult;
import com.alibaba.rocketmq.client.consumer.PullTaskCallback;
import com.alibaba.rocketmq.client.consumer.PullTaskContext;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.common.message.MessageQueue;
import com.alibaba.rocketmq.common.protocol.heartbeat.MessageModel;


public class PullcheduleService {

    public static void main(String[] args) throws MQClientException {
        final DefaultMQPullConsumer consumer =
                new DefaultMQPullConsumer("please_rename_unique_group_name_04");
        MQPullConsumerScheduleService scheduleService = new MQPullConsumerScheduleService(consumer);

        consumer.setMessageModel(MessageModel.CLUSTERING);

        scheduleService.registerPullTaskCallback("TopicTest1", new PullTaskCallback() {

            @Override
            public void doPullTask(MessageQueue mq, PullTaskContext context) {
                try {
                    long offset = consumer.fetchConsumeOffset(mq, false);
                    if (offset < 0)
                        offset = 0;

                    PullResult pullResult = consumer.pullBlockIfNotFound(mq, null, offset, 32);
                    System.out.println(offset + "\t" + mq + "\t" + pullResult);
                    switch (pullResult.getPullStatus()) {
                    case FOUND:
                        break;
                    case NO_MATCHED_MSG:
                        break;
                    case NO_NEW_MSG:
                    case OFFSET_ILLEGAL:
                        break;
                    default:
                        break;
                    }

                    consumer.updateConsumeOffset(mq, pullResult.getNextBeginOffset());
                }
                catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });

        scheduleService.start();
    }

}
