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

package org.apache.rocketmq.broker.plugin;

import java.lang.reflect.Constructor;
import org.apache.rocketmq.broker.exception.BrokerException;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.store.MessageStore;

/**
 * Factory to load a plugin message store.
 */
public final class MessageStoreFactory {

    /**
     * Creates a new {@link MessageStore} by the store plugin class name specified in {@link BrokerConfig}.
     *
     * @param context Store plugin context.
     * @param messageStore Default message store.
     * @return A new {@link MessageStore} if configured.
     * @throws BrokerException If a plugin cannot be loaded.
     */
    public static MessageStore build(MessageStorePluginContext context,
        MessageStore messageStore) throws BrokerException {
        String pluginClass = context.getBrokerConfig().getMessageStorePlugIn();

        if (pluginClass != null && !pluginClass.trim().isEmpty()) {
            try {
                @SuppressWarnings("unchecked")
                Class<AbstractPluginMessageStore> clazz =
                    (Class<AbstractPluginMessageStore>) Class.forName(pluginClass.trim());
                Constructor<AbstractPluginMessageStore> construct =
                    clazz.getConstructor(MessageStorePluginContext.class, MessageStore.class);
                messageStore = construct.newInstance(context, messageStore);
            } catch (Throwable e) {
                throw new BrokerException(String.format(
                    "Initialize plugin's class %s not found!", pluginClass.trim()), e);
            }
        }

        return messageStore;
    }
}
