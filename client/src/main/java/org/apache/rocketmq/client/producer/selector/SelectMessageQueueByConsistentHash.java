/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.client.producer.selector;

import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;

import java.util.HashMap;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

/**
* wanting to send a message with the same key combine with orderly consumer, user can 
* use the implements of the MessageQueueSelector, which has one optimization compared with SelectMessageQueueByHash
* when a broker crash , it can reduce the message Arriving out of the order
*/
public class SelectMessageQueueByConsistentHash implements MessageQueueSelector {

    private volatile SortedMap<Integer, String> virtualNodes =
            new TreeMap<Integer, String>();

    private static final int DEFAULT_VIRTUAL_NODES = 100;

    private final int virtualNodeNum;

    private volatile HashMap<String, MessageQueue> idToQueueMap = new HashMap<String, MessageQueue>();

    public SelectMessageQueueByConsistentHash() {
        this.virtualNodeNum = DEFAULT_VIRTUAL_NODES;
    }

    public SelectMessageQueueByConsistentHash(int virtualNodeNum) {
        this.virtualNodeNum = virtualNodeNum;
    }

    @Override
    public MessageQueue select(List<MessageQueue> mqs, Message msg, Object arg) {
        synchronized (this) {
            if (queueChange(mqs)) {
                reloadConsistentHash(mqs);
            }

            String uniqueQueueId = getMsgQueueIdBy(arg.toString());
            MessageQueue messageQueue = idToQueueMap.get(uniqueQueueId);
            return messageQueue;
        }
    }

    private boolean queueChange(List<MessageQueue> mqs) {
        if (mqs.size() != this.idToQueueMap.size()) {
            return true;
        }

        for (MessageQueue queue : mqs) {
            String id = queue.getTopic() + "_" + queue.getBrokerName() + "_" + queue.getQueueId();
            if (!this.idToQueueMap.containsKey(id)) {
                return true;
            }
        }

        return false;
    }

    private String getMsgQueueIdBy(String arg) {
        int hash = getHash(arg);
        SortedMap<Integer, String> subMap = virtualNodes.tailMap(hash);

        Integer i;
        String virtualNode;

        if (subMap.size() == 0) {
            i = virtualNodes.firstKey();
            virtualNode = virtualNodes.get(i);
        } else {
            i = subMap.firstKey();
            virtualNode = subMap.get(i);
        }

        String result = virtualNode.substring(0, virtualNode.indexOf("&&"));
        return result;
    }

    private int getHash(String str) {
        final int p = 16777619;
        int hash = (int) 2166136261L;
        for (int i = 0; i < str.length(); i++)
            hash = (hash ^ str.charAt(i)) * p;

        hash += hash << 13;
        hash ^= hash >> 7;
        hash += hash << 3;
        hash ^= hash >> 17;
        hash += hash << 5;

        if (hash < 0)
            hash = Math.abs(hash);
        return hash;
    }

    private void reloadConsistentHash(List<MessageQueue> mqs) {
        idToQueueMap.clear();
        for (MessageQueue messageQueue : mqs) {
            String id = messageQueue.getTopic() + "_" + messageQueue.getBrokerName() + "_" + messageQueue.getQueueId();
            idToQueueMap.put(id, messageQueue);
        }

        virtualNodes.clear();
        for (String id : idToQueueMap.keySet()) {
            for (int i = 0; i < virtualNodeNum; i++) {
                String virtualNodeName = id + "&&VN" + String.valueOf(i);
                int hash = getHash(virtualNodeName);
                virtualNodes.put(hash, virtualNodeName);
            }
        }
    }
}
