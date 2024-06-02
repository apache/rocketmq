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
package org.apache.rocketmq.client.impl.consumer;

import org.apache.rocketmq.client.consumer.DefaultLitePullConsumer;
import org.apache.rocketmq.common.message.MessageQueue;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashSet;
import java.util.Set;


public class DefaultLitePullConsumerImplTest {
    private final DefaultLitePullConsumerImpl consumer = new DefaultLitePullConsumerImpl(new DefaultLitePullConsumer(), null);

    private static Method isSetEqualMethod;

    @BeforeClass
    public static void initReflectionMethod() throws NoSuchMethodException {
        Class<DefaultLitePullConsumerImpl> consumerClass = DefaultLitePullConsumerImpl.class;
        Method testMethod = consumerClass.getDeclaredMethod("isSetEqual", Set.class, Set.class);
        testMethod.setAccessible(true);
        isSetEqualMethod = testMethod;
    }


    /**
     * The two empty sets should be equal
     */
    @Test
    public void testIsSetEqual1() throws InvocationTargetException, IllegalAccessException {
        Set<MessageQueue> set1 = new HashSet<>();
        Set<MessageQueue> set2 = new HashSet<>();
        boolean equalResult = (boolean) isSetEqualMethod.invoke(consumer, set1, set2);
        Assert.assertTrue(equalResult);
    }


    /**
     * When a set has elements and one does not, the two sets are not equal
     */
    @Test
    public void testIsSetEqual2() throws InvocationTargetException, IllegalAccessException {
        Set<MessageQueue> set1 = new HashSet<>();
        set1.add(new MessageQueue("testTopic","testBroker",111));
        Set<MessageQueue> set2 = new HashSet<>();
        boolean equalResult = (boolean) isSetEqualMethod.invoke(consumer, set1, set2);
        Assert.assertFalse(equalResult);
    }

    /**
     * The two null sets should be equal
     */
    @Test
    public void testIsSetEqual3() throws InvocationTargetException, IllegalAccessException {
        Set<MessageQueue> set1 = null;
        Set<MessageQueue> set2 = null;
        boolean equalResult = (boolean) isSetEqualMethod.invoke(consumer, set1, set2);
        Assert.assertTrue(equalResult);
    }

    @Test
    public void testIsSetEqual4() throws InvocationTargetException, IllegalAccessException {
        Set<MessageQueue> set1 = null;
        Set<MessageQueue> set2 = new HashSet<>();
        boolean equalResult = (boolean) isSetEqualMethod.invoke(consumer, set1, set2);
        Assert.assertFalse(equalResult);
    }

    @Test
    public void testIsSetEqual5() throws InvocationTargetException, IllegalAccessException {
        Set<MessageQueue> set1 = new HashSet<>();
        set1.add(new MessageQueue("testTopic","testBroker",111));
        Set<MessageQueue> set2 = new HashSet<>();
        set2.add(new MessageQueue("testTopic","testBroker",111));
        boolean equalResult = (boolean) isSetEqualMethod.invoke(consumer, set1, set2);
        Assert.assertTrue(equalResult);
    }

}
