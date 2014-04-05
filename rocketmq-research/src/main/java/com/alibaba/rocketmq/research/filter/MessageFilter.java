package com.alibaba.rocketmq.research.filter;

import com.alibaba.rocketmq.common.message.MessageExt;


public interface MessageFilter {
    public MessageExt doFilter(final MessageExt msg);

}
