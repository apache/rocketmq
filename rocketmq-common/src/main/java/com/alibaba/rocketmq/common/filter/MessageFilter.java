package com.alibaba.rocketmq.common.filter;

import com.alibaba.rocketmq.common.message.MessageExt;


/**
 * 服务端消息过滤接口，Consumer实现这个接口后，Consumer客户端会注册这段Java程序到Broker，由Broker来编译并执行，
 * 以达到服务器消息过滤的目的
 */
public interface MessageFilter {
    /**
     * 过滤消息
     * 
     * @param msg
     * @return 是否可以被Consumer消费
     */
    public boolean match(final MessageExt msg);
}
