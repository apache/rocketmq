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
package io.openmessaging.rocketmq.utils;

import io.openmessaging.BytesMessage;
import io.openmessaging.KeyValue;
import io.openmessaging.MessageHeader;
import io.openmessaging.OMS;
import io.openmessaging.SendResult;
import io.openmessaging.rocketmq.domain.BytesMessageImpl;
import io.openmessaging.rocketmq.domain.NonStandardKeys;
import io.openmessaging.rocketmq.domain.SendResultImpl;
import java.lang.reflect.Field;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.message.MessageAccessor;

public class OMSUtil {

    /**
     * Builds a OMS client instance name.
     *
     * @return a unique instance name
     */
    public static String buildInstanceName() {
        return Integer.toString(UtilAll.getPid()) + "%OpenMessaging" + "%" + System.nanoTime();
    }

    public static org.apache.rocketmq.common.message.Message msgConvert(BytesMessage omsMessage) {
        org.apache.rocketmq.common.message.Message rmqMessage = new org.apache.rocketmq.common.message.Message();
        rmqMessage.setBody(omsMessage.getBody());

        KeyValue headers = omsMessage.headers();
        KeyValue properties = omsMessage.properties();

        //All destinations in RocketMQ use Topic
        if (headers.containsKey(MessageHeader.TOPIC)) {
            rmqMessage.setTopic(headers.getString(MessageHeader.TOPIC));
            rmqMessage.putUserProperty(NonStandardKeys.MESSAGE_DESTINATION, "TOPIC");
        } else {
            rmqMessage.setTopic(headers.getString(MessageHeader.QUEUE));
            rmqMessage.putUserProperty(NonStandardKeys.MESSAGE_DESTINATION, "QUEUE");
        }

        for (String key : properties.keySet()) {
            MessageAccessor.putProperty(rmqMessage, key, properties.getString(key));
        }

        //Headers has a high priority
        for (String key : headers.keySet()) {
            MessageAccessor.putProperty(rmqMessage, key, headers.getString(key));
        }

        return rmqMessage;
    }

    public static BytesMessage msgConvert(org.apache.rocketmq.common.message.MessageExt rmqMsg) {
        BytesMessage omsMsg = new BytesMessageImpl();
        omsMsg.setBody(rmqMsg.getBody());

        KeyValue headers = omsMsg.headers();
        KeyValue properties = omsMsg.properties();

        final Set<Map.Entry<String, String>> entries = rmqMsg.getProperties().entrySet();

        for (final Map.Entry<String, String> entry : entries) {
            if (isOMSHeader(entry.getKey())) {
                headers.put(entry.getKey(), entry.getValue());
            } else {
                properties.put(entry.getKey(), entry.getValue());
            }
        }

        omsMsg.putHeaders(MessageHeader.MESSAGE_ID, rmqMsg.getMsgId());
        if (!rmqMsg.getProperties().containsKey(NonStandardKeys.MESSAGE_DESTINATION) ||
            rmqMsg.getProperties().get(NonStandardKeys.MESSAGE_DESTINATION).equals("TOPIC")) {
            omsMsg.putHeaders(MessageHeader.TOPIC, rmqMsg.getTopic());
        } else {
            omsMsg.putHeaders(MessageHeader.QUEUE, rmqMsg.getTopic());
        }
        omsMsg.putHeaders(MessageHeader.SEARCH_KEY, rmqMsg.getKeys());
        omsMsg.putHeaders(MessageHeader.BORN_HOST, String.valueOf(rmqMsg.getBornHost()));
        omsMsg.putHeaders(MessageHeader.BORN_TIMESTAMP, rmqMsg.getBornTimestamp());
        omsMsg.putHeaders(MessageHeader.STORE_HOST, String.valueOf(rmqMsg.getStoreHost()));
        omsMsg.putHeaders(MessageHeader.STORE_TIMESTAMP, rmqMsg.getStoreTimestamp());
        return omsMsg;
    }

    public static boolean isOMSHeader(String value) {
        for (Field field : MessageHeader.class.getDeclaredFields()) {
            try {
                if (field.get(MessageHeader.class).equals(value)) {
                    return true;
                }
            } catch (IllegalAccessException e) {
                return false;
            }
        }
        return false;
    }

    /**
     * Convert a RocketMQ SEND_OK SendResult instance to a OMS SendResult.
     */
    public static SendResult sendResultConvert(org.apache.rocketmq.client.producer.SendResult rmqResult) {
        assert rmqResult.getSendStatus().equals(SendStatus.SEND_OK);
        return new SendResultImpl(rmqResult.getMsgId(), OMS.newKeyValue());
    }

    public static KeyValue buildKeyValue(KeyValue... keyValues) {
        KeyValue keyValue = OMS.newKeyValue();
        for (KeyValue properties : keyValues) {
            for (String key : properties.keySet()) {
                keyValue.put(key, properties.getString(key));
            }
        }
        return keyValue;
    }

    /**
     * Returns an iterator that cycles indefinitely over the elements of {@code Iterable}.
     */
    public static <T> Iterator<T> cycle(final Iterable<T> iterable) {
        return new Iterator<T>() {
            Iterator<T> iterator = new Iterator<T>() {
                @Override
                public synchronized boolean hasNext() {
                    return false;
                }

                @Override
                public synchronized T next() {
                    throw new NoSuchElementException();
                }

                @Override
                public synchronized void remove() {
                    //Ignore
                }
            };

            @Override
            public synchronized boolean hasNext() {
                return iterator.hasNext() || iterable.iterator().hasNext();
            }

            @Override
            public synchronized T next() {
                if (!iterator.hasNext()) {
                    iterator = iterable.iterator();
                    if (!iterator.hasNext()) {
                        throw new NoSuchElementException();
                    }
                }
                return iterator.next();
            }

            @Override
            public synchronized void remove() {
                iterator.remove();
            }
        };
    }
}
