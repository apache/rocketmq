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

import java.io.IOException;
import java.lang.reflect.Constructor;
import org.apache.rocketmq.store.MessageStore;

public final class MessageStoreFactory {
    public final static MessageStore build(MessageStorePluginContext context,
        MessageStore messageStore) throws IOException {
        String plugin = context.getBrokerConfig().getMessageStorePlugIn();
        if (plugin != null && plugin.trim().length() != 0) {
            String[] pluginClasses = plugin.split(",");
            for (int i = pluginClasses.length - 1; i >= 0; --i) {
                String pluginClass = pluginClasses[i];
                try {
                    @SuppressWarnings("unchecked")
                    Class<AbstractPluginMessageStore> clazz = (Class<AbstractPluginMessageStore>)Class.forName(pluginClass);
                    Constructor<AbstractPluginMessageStore> construct = clazz.getConstructor(MessageStorePluginContext.class, MessageStore.class);
                    AbstractPluginMessageStore pluginMessageStore = construct.newInstance(context, messageStore);
                    messageStore = pluginMessageStore;
                }
                catch (Throwable e) {
                    throw new RuntimeException("Initialize plugin's class: " + pluginClass + " not found!", e);
                }
            }
        }
        return messageStore;
    }
}
