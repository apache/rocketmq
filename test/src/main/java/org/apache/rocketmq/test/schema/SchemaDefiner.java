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
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.apache.rocketmq.test.schema;

import com.google.common.collect.Sets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.rocketmq.client.ClientConfig;
import org.apache.rocketmq.client.consumer.AllocateMessageQueueStrategy;
import org.apache.rocketmq.client.consumer.DefaultLitePullConsumer;
import org.apache.rocketmq.client.consumer.DefaultMQPullConsumer;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.PullCallback;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.client.consumer.PullStatus;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListener;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.CommandCustomHeader;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.protocol.RequestCode;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.reflections.Reflections;

public class SchemaDefiner {
    public static final Map<Class<?>, Set<String>> IGNORED_FIELDS = new HashMap<>();
    //Use name as the key instead of X.class directly. X.class is not equal to field.getType().
    public static final Set<String> FIELD_CLASS_NAMES = new HashSet<>();
    public static final List<Class<?>> API_CLASS_LIST = new ArrayList<>();
    public static final List<Class<?>> PROTOCOL_CLASS_LIST = new ArrayList<>();

    public static void doLoad() {
        {
            IGNORED_FIELDS.put(ClientConfig.class, Sets.newHashSet("namesrvAddr", "clientIP", "clientCallbackExecutorThreads"));
            IGNORED_FIELDS.put(DefaultLitePullConsumer.class, Sets.newHashSet("consumeTimestamp"));
            IGNORED_FIELDS.put(DefaultMQPushConsumer.class, Sets.newHashSet("consumeTimestamp"));
            FIELD_CLASS_NAMES.add(String.class.getName());
            FIELD_CLASS_NAMES.add(Long.class.getName());
            FIELD_CLASS_NAMES.add(Integer.class.getName());
            FIELD_CLASS_NAMES.add(Short.class.getName());
            FIELD_CLASS_NAMES.add(Byte.class.getName());
            FIELD_CLASS_NAMES.add(Double.class.getName());
            FIELD_CLASS_NAMES.add(Float.class.getName());
            FIELD_CLASS_NAMES.add(Boolean.class.getName());
        }
        {
            //basic
            API_CLASS_LIST.add(DefaultMQPushConsumer.class);
            API_CLASS_LIST.add(DefaultMQProducer.class);
            API_CLASS_LIST.add(DefaultMQPullConsumer.class);
            API_CLASS_LIST.add(DefaultLitePullConsumer.class);
            API_CLASS_LIST.add(DefaultMQAdminExt.class);

            //argument
            API_CLASS_LIST.add(Message.class);
            API_CLASS_LIST.add(MessageQueue.class);
            API_CLASS_LIST.add(SendCallback.class);
            API_CLASS_LIST.add(PullCallback.class);
            API_CLASS_LIST.add(MessageQueueSelector.class);
            API_CLASS_LIST.add(AllocateMessageQueueStrategy.class);
            //result
            API_CLASS_LIST.add(MessageExt.class);
            API_CLASS_LIST.add(SendResult.class);
            API_CLASS_LIST.add(SendStatus.class);
            API_CLASS_LIST.add(PullResult.class);
            API_CLASS_LIST.add(PullStatus.class);
            //listener and context
            API_CLASS_LIST.add(MessageListener.class);
            API_CLASS_LIST.add(MessageListenerConcurrently.class);
            API_CLASS_LIST.add(ConsumeConcurrentlyContext.class);
            API_CLASS_LIST.add(ConsumeConcurrentlyStatus.class);
            API_CLASS_LIST.add(MessageListenerOrderly.class);
            API_CLASS_LIST.add(ConsumeOrderlyContext.class);
            API_CLASS_LIST.add(ConsumeOrderlyStatus.class);
            //hook and context
            API_CLASS_LIST.add(RPCHook.class);
            API_CLASS_LIST.add(org.apache.rocketmq.client.hook.FilterMessageHook.class);
            API_CLASS_LIST.add(org.apache.rocketmq.client.hook.SendMessageHook.class);
            API_CLASS_LIST.add(org.apache.rocketmq.client.hook.CheckForbiddenHook.class);
            API_CLASS_LIST.add(org.apache.rocketmq.client.hook.ConsumeMessageHook.class);
            API_CLASS_LIST.add(org.apache.rocketmq.client.hook.EndTransactionHook.class);
            API_CLASS_LIST.add(org.apache.rocketmq.client.hook.FilterMessageContext.class);
            API_CLASS_LIST.add(org.apache.rocketmq.client.hook.SendMessageContext.class);
            API_CLASS_LIST.add(org.apache.rocketmq.client.hook.ConsumeMessageContext.class);
            API_CLASS_LIST.add(org.apache.rocketmq.client.hook.ConsumeMessageContext.class);
            API_CLASS_LIST.add(org.apache.rocketmq.client.hook.EndTransactionContext.class);

        }
        {
            PROTOCOL_CLASS_LIST.add(RequestCode.class);
            Reflections reflections = new Reflections("org.apache.rocketmq");
            for (Class<?> protocolClass: reflections.getSubTypesOf(CommandCustomHeader.class)) {
                PROTOCOL_CLASS_LIST.add(protocolClass);
            }
        }

    }

}
