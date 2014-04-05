package com.alibaba.rocketmq.research.filter;

import com.alibaba.rocketmq.common.message.MessageExt;


public class MessageFilterImpl implements MessageFilter {

    @Override
    public MessageExt doFilter(MessageExt msg) {
        System.out.println("MessageFilterImpl doFilter");
        return null;
    }

}
