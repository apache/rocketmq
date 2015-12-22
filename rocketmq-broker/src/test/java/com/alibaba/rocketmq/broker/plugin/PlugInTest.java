/**
 * 
 */
package com.alibaba.rocketmq.broker.plugin;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.alibaba.rocketmq.common.BrokerConfig;
import com.alibaba.rocketmq.store.MessageStore;

/**
 * @author qinan.qn@taobao.com 2015年12月12日
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
    public void testMessageStoreFactory() throws IOException{
        BrokerConfig brokerConfig = new BrokerConfig();
        MessageStorePluginContext context=  new MessageStorePluginContext(null, null, null, brokerConfig);
        MessageStore messageStore = null;
        AbstractPluginMessageStore m = null;
        //无plugin测试
        brokerConfig.setMessageStorePlugIn(null);
        messageStore = MessageStoreFactory.build(context, new MockMessageStore());
        assertTrue(messageStore instanceof MockMessageStore);
        
        brokerConfig.setMessageStorePlugIn("");
        messageStore = MessageStoreFactory.build(context, new MockMessageStore());
        assertTrue(messageStore instanceof MockMessageStore);
        
        //有plugin测试
        brokerConfig.setMessageStorePlugIn("com.alibaba.rocketmq.broker.plugin.MockMessageStorePlugin1,com.alibaba.rocketmq.broker.plugin.MockMessageStorePlugin2");
        messageStore = MessageStoreFactory.build(context,new MockMessageStore());
        assertTrue(messageStore instanceof MockMessageStorePlugin1);
        m = (AbstractPluginMessageStore)messageStore;
        
        assertTrue(m.next instanceof MockMessageStorePlugin2);
        m = (AbstractPluginMessageStore)m.next;
        assertTrue(m.next instanceof MockMessageStore);
        
        //抛出异常测试
        brokerConfig.setMessageStorePlugIn("aaaaaa");
        try{
            messageStore = MessageStoreFactory.build(context,new MockMessageStore());
            assertTrue(false);
        }catch(RuntimeException e){
        }
        
    }
    
    @Test
    public void testAbstractPluginMessageStore() throws IllegalAccessException, IllegalArgumentException,
            InvocationTargetException {

        
        final ThreadLocal<String> invokingMethodName = new ThreadLocal<String>();
        
        MessageStore messageStore = (MessageStore) Proxy.newProxyInstance(
                MessageStore.class.getClassLoader(), new Class[] { MessageStore.class },
                new InvocationHandler() {
                    @Override
                    public Object invoke(Object proxy, Method method, Object[] args)
                            throws Throwable {
                        assertEquals(invokingMethodName.get(), method.getName());
                        if (method.getReturnType() == int.class){
                            return new Integer(0);
                        }else if( method.getReturnType() == long.class){
                            return new Long((byte)0);
                        }else if(method.getReturnType() == char.class){
                            return new Byte((byte)0);
                        }else if( method.getReturnType() == byte.class){
                            return new Byte((byte)0);
                        } else if (method.getReturnType() == boolean.class) {
                            return true;
                        } else {
                            return null;
                        }
                    }
                });

        AbstractPluginMessageStore pluginMessageStore = new AbstractPluginMessageStore(null, messageStore) {
            
        };

        Method[] methods = MessageStore.class.getMethods();
        for (Method m : methods) {
            Class[] paramType = m.getParameterTypes();
            Object[] mockParam = new Object[paramType.length];
            for (int i = 0; i < paramType.length; ++i) {
                if (paramType[i] == int.class) {
                    mockParam[i] = new Integer(0);
                } else if (paramType[i] == long.class) {
                    mockParam[i] = new Long(0);
                } else if (paramType[i] == byte.class) {
                    mockParam[i] = new Byte((byte) 0);
                } else if (paramType[i] == char.class) {
                    mockParam[i] = new Byte((byte) 0);
                } else
                    mockParam[i] = null;
            }
            invokingMethodName.set(m.getName());
            m.invoke(pluginMessageStore, mockParam);
        }
    }

}
