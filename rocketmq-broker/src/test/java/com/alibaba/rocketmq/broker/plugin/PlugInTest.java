/**
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

/**
 *
 */
package com.alibaba.rocketmq.broker.plugin;

import com.alibaba.rocketmq.common.BrokerConfig;
import com.alibaba.rocketmq.store.MessageStore;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author qinan.qn@taobao.com 2015��12��12��
 */
public class PlugInTest {

    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception {
    }

    /**
     * @throws java.lang.Exception
     */
    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void testMessageStoreFactory() throws IOException {
        BrokerConfig brokerConfig = new BrokerConfig();
        MessageStorePluginContext context = new MessageStorePluginContext(null, null, null, brokerConfig);
        MessageStore messageStore = null;
        AbstractPluginMessageStore m = null;
        //��plugin����
        brokerConfig.setMessageStorePlugIn(null);
        messageStore = MessageStoreFactory.build(context, new MockMessageStore());
        assertTrue(messageStore instanceof MockMessageStore);

        brokerConfig.setMessageStorePlugIn("");
        messageStore = MessageStoreFactory.build(context, new MockMessageStore());
        assertTrue(messageStore instanceof MockMessageStore);

        //��plugin����
        brokerConfig.setMessageStorePlugIn("com.alibaba.rocketmq.broker.plugin.MockMessageStorePlugin1,com.alibaba.rocketmq.broker.plugin.MockMessageStorePlugin2");
        messageStore = MessageStoreFactory.build(context, new MockMessageStore());
        assertTrue(messageStore instanceof MockMessageStorePlugin1);
        m = (AbstractPluginMessageStore) messageStore;

        assertTrue(m.next instanceof MockMessageStorePlugin2);
        m = (AbstractPluginMessageStore) m.next;
        assertTrue(m.next instanceof MockMessageStore);

        //�׳��쳣����
        brokerConfig.setMessageStorePlugIn("aaaaaa");
        try {
            messageStore = MessageStoreFactory.build(context, new MockMessageStore());
            assertTrue(false);
        } catch (RuntimeException e) {
        }

    }

    @Test
    public void testAbstractPluginMessageStore() throws IllegalAccessException, IllegalArgumentException,
            InvocationTargetException {


        final ThreadLocal<String> invokingMethodName = new ThreadLocal<String>();

        MessageStore messageStore = (MessageStore) Proxy.newProxyInstance(
                MessageStore.class.getClassLoader(), new Class[]{MessageStore.class},
                new InvocationHandler() {
                    @Override
                    public Object invoke(Object proxy, Method method, Object[] args)
                            throws Throwable {
                        assertEquals(invokingMethodName.get(), method.getName());
                        if (method.getReturnType() == int.class) {
                            return Integer.valueOf(0);
                        } else if (method.getReturnType() == long.class) {
                            return Long.valueOf(0);
                        } else if (method.getReturnType() == char.class) {
                            return Byte.valueOf((byte) 0);
                        } else if (method.getReturnType() == byte.class) {
                            return Byte.valueOf((byte) 0);
                        } else if (method.getReturnType() == boolean.class) {
                            return true;
                        } else {
                            return null;
                        }
                    }
                });

        AbstractPluginMessageStore pluginMessageStore = new AbstractPluginMessageStore(null, messageStore) {

            @Override
            public boolean isOSPageCacheBusy() {
                return false;
            }
        };

        Method[] methods = MessageStore.class.getMethods();
        for (Method m : methods) {
            Class[] paramType = m.getParameterTypes();
            Object[] mockParam = new Object[paramType.length];
            for (int i = 0; i < paramType.length; ++i) {
                if (paramType[i] == int.class) {
                    mockParam[i] = Integer.valueOf(0);
                } else if (paramType[i] == long.class) {
                    mockParam[i] = Long.valueOf(0);
                } else if (paramType[i] == byte.class) {
                    mockParam[i] = Byte.valueOf((byte)0);
                } else if (paramType[i] == char.class) {
                    mockParam[i] = Byte.valueOf((byte)0);
                } else
                    mockParam[i] = null;
            }
            invokingMethodName.set(m.getName());
            m.invoke(pluginMessageStore, mockParam);
        }
    }

}
