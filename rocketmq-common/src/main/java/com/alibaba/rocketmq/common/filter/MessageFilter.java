package com.alibaba.rocketmq.common.filter;

import com.alibaba.rocketmq.common.message.MessageExt;


public interface MessageFilter {
    public boolean match(final MessageExt msg);
}
