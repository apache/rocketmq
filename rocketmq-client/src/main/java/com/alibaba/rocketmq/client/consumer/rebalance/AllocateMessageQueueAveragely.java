/**
 * Copyright (C) 2010-2013 Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
 * @since 2013-7-24
 */
public class AllocateMessageQueueAveragely implements AllocateMessageQueueStrategy {
    @Override
    public List<MessageQueue> allocate(String currentCID, List<MessageQueue> mqAll, List<String> cidAll) {
        if (currentCID == null || currentCID.length() < 1) {
            throw new IllegalArgumentException("currentCID is empty");
        }
        if (mqAll == null || mqAll.size() < 1) {
            throw new IllegalArgumentException("mqAll is null or  mqAll'size  less one");
        }
        if (cidAll == null || cidAll.size() < 1) {
            throw new IllegalArgumentException("cidAll is null or  cidAll'size less one");
        }

        List<MessageQueue> result = new ArrayList<MessageQueue>();
        if (!cidAll.contains(currentCID)) { // 不存在此ConsumerId ,直接返回
            return result;
        }

        int index = cidAll.indexOf(currentCID);
        int averageSize = mqAll.size() / cidAll.size();
        int mod = mqAll.size() % cidAll.size();
        int startIndex = index * averageSize;
        int endIndex = (index + 1) * averageSize;
        for (int i = startIndex; i < endIndex; i++) {
            result.add(mqAll.get(i));
        }

        // 如果当前的consumerId最后一个且还有剩下的队列，应该把最后队列都放到当前consumerId队列里
        boolean isAddRemainQueue = (index == cidAll.size() - 1) && mod > 0;
        if (isAddRemainQueue) {
            int messageQueueSize = mqAll.size();
            for (int i = endIndex; i < messageQueueSize; i++) {
                result.add(mqAll.get(i));
            }
        }
        return result;
    }
}
