package com.alibaba.rocketmq.example.filter;

import com.alibaba.rocketmq.common.filter.MessageFilter;
import com.alibaba.rocketmq.common.message.MessageExt;


public class MessageFilterImpl implements MessageFilter {

    @Override
    public boolean match(MessageExt msg) {
        String property = msg.getProperty("SequenceId");
        if (property != null) {
            int id = Integer.parseInt(property);
            if ((id % 2) == 0) {
                return true;
            }
        }

        return false;
    }
}
