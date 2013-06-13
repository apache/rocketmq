/**
 * $Id: AllocateMessageQueueAveragely.java 1831 2013-05-16 01:39:51Z shijia.wxr $
 */
package com.alibaba.rocketmq.client.consumer.rebalance;

import java.util.ArrayList;
import java.util.List;

import com.alibaba.rocketmq.client.consumer.AllocateMessageQueueStrategy;
import com.alibaba.rocketmq.common.message.MessageQueue;


/**
 * 平均分配队列算法
 * 
 * @author shijia.wxr<vintage.wang@gmail.com>
 * 
 */
public class AllocateMessageQueueAveragely implements AllocateMessageQueueStrategy {
    @Override
    public List<MessageQueue> allocate(String currentCID, List<MessageQueue> mqAll, List<String> cidAll) {
        List<MessageQueue> result = new ArrayList<MessageQueue>();
        int currentIndex = cidAll.indexOf(currentCID);
        if (currentIndex < 0) {
            return result;
        }

        int mod = mqAll.size() / cidAll.size();
        int rem = mqAll.size() % cidAll.size();
        int startindex = mod * currentIndex;
        int endindex = startindex + mod;
        for (int i = startindex; i < endindex; i++) {
            result.add(mqAll.get(i));
        }
        if (rem > currentIndex) {
            result.add(mqAll.get(currentIndex + mod * cidAll.size()));
        }
        return result;
    }
}
