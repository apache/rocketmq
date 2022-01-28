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
package org.apache.rocketmq.common.message;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.rocketmq.common.MixAll;

public class MessageBatch extends Message implements Iterable<Message> {

    private static final long serialVersionUID = 621335151046335557L;
    private final List<Message> messages;

    private boolean multiTopic = false;

    private Map<String/*topic*/, Integer/*index*/> topicIndexMap;
    private Map<String/*topic*/, Integer/*queueId*/> queueIdMap;

    private MessageBatch(List<Message> messages) {
        this.messages = messages;
    }

    private MessageBatch(List<Message> messages, boolean multiTopic) {
        this.messages = messages;
        this.multiTopic = multiTopic;
    }

    public boolean isMultiTopic() {
        return multiTopic;
    }

    public void setTopicIndexMap(Map<String, Integer> topicIndexMap) {
        this.topicIndexMap = topicIndexMap;
    }

    public Map<String, Integer> getTopicIndexMap() {
        return topicIndexMap;
    }

    public Map<String, Integer> getQueueIdMap() {
        return queueIdMap;
    }

    public void setQueueIdMap(Map<String, Integer> queueIdMap) {
        this.queueIdMap = queueIdMap;
    }

    public byte[] encode() {
        if (multiTopic) {
            return MessageDecoder.encodeMultiTopicMessages(messages, topicIndexMap);
        } else {
            return MessageDecoder.encodeMessages(messages);
        }
    }

    public Iterator<Message> iterator() {
        return messages.iterator();
    }

    public static MessageBatch generateFromList(Collection<Message> messages, boolean allowMultiTopic) {
        assert messages != null;
        assert messages.size() > 0;
        List<Message> messageList = new ArrayList<Message>(messages.size());
        Message first = null;
        boolean multiTopic = false;
        Set<String> topics = new HashSet<>();
        for (Message message : messages) {
            if (message.getDelayTimeLevel() > 0) {
                throw new UnsupportedOperationException("TimeDelayLevel is not supported for batching");
            }
            if (message.getTopic().startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX)) {
                throw new UnsupportedOperationException("Retry Group is not supported for batching");
            }
            if (first == null) {
                first = message;
            } else {
                if (!first.getTopic().equals(message.getTopic())) {
                    if (!allowMultiTopic) {
                        throw new UnsupportedOperationException("The topic of the messages in one batch should be the same");
                    }
                    multiTopic = true;
                }
                if (first.isWaitStoreMsgOK() != message.isWaitStoreMsgOK()) {
                    throw new UnsupportedOperationException("The waitStoreMsgOK of the messages in one batch should the same");
                }
            }
            messageList.add(message);
            topics.add(message.getTopic());
        }

        MessageBatch messageBatch = new MessageBatch(messageList, multiTopic);

        if (multiTopic) {
            Map<String, Integer> topicIndexMap = new HashMap<>(topics.size());
            int index = 0;
            StringBuilder sb = new StringBuilder();
            for (String topic : topics) {
                if (index != 0) {
                    sb.append(MixAll.BATCH_TOPIC_SPLITTER);
                }
                sb.append(topic);
                topicIndexMap.put(topic, index);
                index++;
            }
            messageBatch.setTopic(sb.toString());
            messageBatch.setTopicIndexMap(topicIndexMap);
        } else {
            messageBatch.setTopic(first.getTopic());
        }
        messageBatch.setWaitStoreMsgOK(first.isWaitStoreMsgOK());

        return messageBatch;
    }
}
