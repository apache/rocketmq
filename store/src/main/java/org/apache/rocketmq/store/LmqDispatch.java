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
package org.apache.rocketmq.store;

import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageExtBrokerInner;
import org.apache.rocketmq.store.exception.ConsumeQueueException;

public class LmqDispatch {
    private static final short VALUE_OF_EACH_INCREMENT = 1;

    public static void wrapLmqDispatch(MessageStore messageStore, final MessageExtBrokerInner msg)
        throws ConsumeQueueException {
        populateLmqOffsets(messageStore, msg);
        msg.removeWaitStorePropertyString();
    }

    static String[] prepareLmqDispatch(MessageStore messageStore, final MessageExtBrokerInner msg)
        throws ConsumeQueueException {
        return populateLmqOffsets(messageStore, msg);
    }

    static void reinsertWaitStorePropertyForLegacySerialization(final MessageExtBrokerInner msg) {
        // Reproduce the legacy remove/reinsert mutation without the discarded serialization.
        if (msg.getProperties().containsKey(MessageConst.PROPERTY_WAIT_STORE_MSG_OK)) {
            String waitStoreMsgOKValue = msg.getProperties().remove(MessageConst.PROPERTY_WAIT_STORE_MSG_OK);
            msg.getProperties().put(MessageConst.PROPERTY_WAIT_STORE_MSG_OK, waitStoreMsgOKValue);
        }
    }

    private static String[] populateLmqOffsets(MessageStore messageStore, final MessageExtBrokerInner msg)
        throws ConsumeQueueException {
        String[] queueNames = parseLmqQueueNames(msg);
        StringBuilder queueOffsets = new StringBuilder();
        boolean enableLmq = messageStore.getMessageStoreConfig().isEnableLmq();
        for (int i = 0; i < queueNames.length; i++) {
            if (i > 0) {
                queueOffsets.append(MixAll.LMQ_DISPATCH_SEPARATOR);
            }
            if (enableLmq && MixAll.isLmq(queueNames[i])) {
                queueOffsets.append(messageStore.getQueueStore().getLmqQueueOffset(queueNames[i],
                    MixAll.LMQ_QUEUE_ID));
            }
        }
        MessageAccessor.putProperty(msg, MessageConst.PROPERTY_INNER_MULTI_QUEUE_OFFSET, queueOffsets.toString());
        return queueNames;
    }

    private static String[] parseLmqQueueNames(final MessageExtBrokerInner msg) {
        String lmqNames = msg.getProperty(MessageConst.PROPERTY_INNER_MULTI_DISPATCH);
        return lmqNames.split(MixAll.LMQ_DISPATCH_SEPARATOR);
    }

    public static void updateLmqOffsets(MessageStore messageStore, final MessageExtBrokerInner msgInner)
        throws ConsumeQueueException {
        updateLmqOffsets(messageStore, parseLmqQueueNames(msgInner));
    }

    static void updateLmqOffsets(MessageStore messageStore, String[] queueNames)
        throws ConsumeQueueException {
        updateLmqOffsets(messageStore, queueNames, VALUE_OF_EACH_INCREMENT);
    }

    static void updateLmqOffsets(MessageStore messageStore, String[] queueNames, short increment)
        throws ConsumeQueueException {
        for (String queueName : queueNames) {
            if (messageStore.getMessageStoreConfig().isEnableLmq() && MixAll.isLmq(queueName)) {
                messageStore.getQueueStore().increaseLmqOffset(queueName, MixAll.LMQ_QUEUE_ID, increment);
            }
        }
    }
}
