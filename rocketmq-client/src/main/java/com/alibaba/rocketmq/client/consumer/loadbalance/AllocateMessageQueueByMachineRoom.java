/**
 * $Id: AllocateMessageQueueByMachineRoom.java 1831 2013-05-16 01:39:51Z shijia.wxr $
 */
package com.alibaba.rocketmq.client.consumer.loadbalance;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import com.alibaba.rocketmq.client.consumer.AllocateMessageQueueStrategy;
import com.alibaba.rocketmq.common.message.MessageQueue;


/**
 * 按照机房来分配队列，例如支付宝逻辑机房
 * 
 * @author linye<jin.qian@alipay.com>
 */
public class AllocateMessageQueueByMachineRoom implements AllocateMessageQueueStrategy {
    private Set<String> consumeridcs;


    @Override
    public List<MessageQueue> allocate(String group, String topic, String currentCID, List<MessageQueue> mqAll,
            List<String> cidAll) {
        List<MessageQueue> result = new ArrayList<MessageQueue>();
        int currentIndex = cidAll.indexOf(currentCID);
        if (currentIndex < 0) {
            return result;
        }
        List<MessageQueue> premqAll = new ArrayList<MessageQueue>();
        for (MessageQueue mq : mqAll) {
            String[] temp = mq.getBrokerName().split("@");
            if (temp.length == 2 && consumeridcs.contains(temp[0])) {
                premqAll.add(mq);
            }
        }
        // Todo cid
        int mod = premqAll.size() / cidAll.size();
        int rem = premqAll.size() % cidAll.size();
        int startindex = mod * currentIndex;
        int endindex = startindex + mod;
        for (int i = startindex; i < endindex; i++) {
            result.add(mqAll.get(i));
        }
        if (rem > currentIndex) {
            result.add(premqAll.get(currentIndex + mod * cidAll.size()));
        }
        return result;
    }


    public Set<String> getConsumeridcs() {
        return consumeridcs;
    }


    public void setConsumeridcs(Set<String> consumeridcs) {
        this.consumeridcs = consumeridcs;
    }
}
