/**
 * $Id: MessageQueueSelector.java 1831 2013-05-16 01:39:51Z shijia.wxr $
 */
package com.alibaba.rocketmq.client.producer;

import java.util.List;

import com.alibaba.rocketmq.common.Message;
import com.alibaba.rocketmq.common.MessageQueue;


/**
 * ¶ÓÁÐÑ¡ÔñÆ÷
 * 
 * @author vintage.wang@gmail.com shijia.wxr@taobao.com
 */
public interface MessageQueueSelector {
    public MessageQueue select(final List<MessageQueue> mqs, final Message msg, final Object arg);
}
