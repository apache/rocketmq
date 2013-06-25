package com.alibaba.rocketmq.client.impl.consumer;

import java.util.List;

import com.alibaba.rocketmq.common.message.MessageExt;
import com.alibaba.rocketmq.common.message.MessageQueue;


/**
 * @author shijia.wxr<vintage.wang@gmail.com>
 */
public interface ConsumeMessageService {
    public void start();


    public void shutdown();


    public void submitConsumeRequest(//
            final List<MessageExt> msgs, //
            final ProcessQueue processQueue, //
            final MessageQueue messageQueue, //
            final boolean isEmptyBefore);
}
