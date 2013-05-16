/**
 * $Id: SelectMessageQueueByMachineRoom.java 1831 2013-05-16 01:39:51Z shijia.wxr $
 */
package com.alibaba.rocketmq.client.producer.selector;

import java.util.List;

import com.alibaba.rocketmq.client.producer.MessageQueueSelector;
import com.alibaba.rocketmq.common.Message;
import com.alibaba.rocketmq.common.MessageQueue;


/**
 * 根据机房来选择发往哪个队列，支付宝逻辑机房使用
 * 
 * @author vintage.wang@gmail.com shijia.wxr@taobao.com
 * 
 */
public class SelectMessageQueueByMachineRoom implements MessageQueueSelector {

    @Override
    public MessageQueue select(List<MessageQueue> mqs, Message msg, Object arg) {
        // TODO Auto-generated method stub
        return null;
    }
}
