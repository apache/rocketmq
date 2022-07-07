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
import org.apache.rocketmq.common.protocol.RequestCode;
import org.apache.rocketmq.remoting.CommandCustomHeader;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.reflections.Reflections;

public class SchemaDefiner {
    public static Map<Class<?>, Set<String>> ignoredFields = new HashMap<>();
    //Use name as the key instead of X.class directly. X.class is not equal to field.getType().
    public static Set<String> fieldClassNames = new HashSet<>();
    public static List<Class<?>> apiClassList = new ArrayList<>();
    public static List<Class<?>> protocolClassList = new ArrayList<>();

    public static void doLoad() {
        {
            ignoredFields.put(ClientConfig.class, Sets.newHashSet("namesrvAddr", "clientIP", "clientCallbackExecutorThreads"));
            ignoredFields.put(DefaultLitePullConsumer.class, Sets.newHashSet("consumeTimestamp"));
            ignoredFields.put(DefaultMQPushConsumer.class, Sets.newHashSet("consumeTimestamp"));
            fieldClassNames.add(String.class.getName());
            fieldClassNames.add(Long.class.getName());
            fieldClassNames.add(Integer.class.getName());
            fieldClassNames.add(Short.class.getName());
            fieldClassNames.add(Byte.class.getName());
            fieldClassNames.add(Double.class.getName());
            fieldClassNames.add(Float.class.getName());
            fieldClassNames.add(Boolean.class.getName());
        }
        {
            //basic
            apiClassList.add(DefaultMQPushConsumer.class);
            apiClassList.add(DefaultMQProducer.class);
            apiClassList.add(DefaultMQPullConsumer.class);
            apiClassList.add(DefaultLitePullConsumer.class);
            apiClassList.add(DefaultMQAdminExt.class);

            //argument
            apiClassList.add(Message.class);
            apiClassList.add(MessageQueue.class);
            apiClassList.add(SendCallback.class);
            apiClassList.add(PullCallback.class);
            apiClassList.add(MessageQueueSelector.class);
            apiClassList.add(AllocateMessageQueueStrategy.class);
            //result
            apiClassList.add(MessageExt.class);
            apiClassList.add(SendResult.class);
            apiClassList.add(SendStatus.class);
            apiClassList.add(PullResult.class);
            apiClassList.add(PullStatus.class);
            //listener and context
            apiClassList.add(MessageListener.class);
            apiClassList.add(MessageListenerConcurrently.class);
            apiClassList.add(ConsumeConcurrentlyContext.class);
            apiClassList.add(ConsumeConcurrentlyStatus.class);
            apiClassList.add(MessageListenerOrderly.class);
            apiClassList.add(ConsumeOrderlyContext.class);
            apiClassList.add(ConsumeOrderlyStatus.class);
            //hook and context
            apiClassList.add(RPCHook.class);
            apiClassList.add(org.apache.rocketmq.client.hook.FilterMessageHook.class);
            apiClassList.add(org.apache.rocketmq.client.hook.SendMessageHook.class);
            apiClassList.add(org.apache.rocketmq.client.hook.CheckForbiddenHook.class);
            apiClassList.add(org.apache.rocketmq.client.hook.ConsumeMessageHook.class);
            apiClassList.add(org.apache.rocketmq.client.hook.EndTransactionHook.class);
            apiClassList.add(org.apache.rocketmq.client.hook.FilterMessageContext.class);
            apiClassList.add(org.apache.rocketmq.client.hook.SendMessageContext.class);
            apiClassList.add(org.apache.rocketmq.client.hook.ConsumeMessageContext.class);
            apiClassList.add(org.apache.rocketmq.client.hook.ConsumeMessageContext.class);
            apiClassList.add(org.apache.rocketmq.client.hook.EndTransactionContext.class);

        }
        {
            protocolClassList.add(RequestCode.class);
            Reflections reflections = new Reflections("org.apache.rocketmq");
            for (Class<?> protocolClass: reflections.getSubTypesOf(CommandCustomHeader.class)) {
                protocolClassList.add(protocolClass);
            }
        }

    }

}
