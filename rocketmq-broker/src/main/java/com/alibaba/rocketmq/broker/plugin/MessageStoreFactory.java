/**
 * 
 */
package com.alibaba.rocketmq.broker.plugin;

import java.io.IOException;
import java.lang.reflect.Constructor;

import com.alibaba.rocketmq.store.MessageStore;

/**
 * @author qinan.qn@taobao.com 2015年12月12日
 */
public final class MessageStoreFactory {
    public final static MessageStore build(MessageStorePluginContext context , MessageStore messageStore)
            throws IOException {
        String plugin = context.getBrokerConfig().getMessageStorePlugIn();
        if (plugin != null && plugin.trim().length() != 0) {
            String[] pluginClasses = plugin.split(",");
            for (int i = pluginClasses.length - 1 ; i >= 0 ; --i) {
                String pluginClass  = pluginClasses[i];
                try {
                    @SuppressWarnings("unchecked")
                    Class<AbstractPluginMessageStore> clazz = (Class<AbstractPluginMessageStore>) Class.forName(pluginClass);
                    Constructor<AbstractPluginMessageStore> construct = clazz.getConstructor(MessageStorePluginContext.class , MessageStore.class);
                    AbstractPluginMessageStore pluginMessageStore = (AbstractPluginMessageStore) construct.newInstance(context,messageStore);
                    messageStore = pluginMessageStore;
                } catch (Throwable e) {
                    throw new RuntimeException(String.format(
                            "Initialize plugin's class %s not found!", pluginClass), e);
                }
            }
        }
        return messageStore;
    }
}
