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

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

/**
 * Tests for CleanCommitLogService.
 */
public class CleanCommitLogServiceTest {
    DefaultMessageStore messageStore;
    DefaultMessageStore.CleanCommitLogService cleanCommitLogService;

    // CleanCommitLogService class.
    Class<?> clazz;

    @Before
    public void setUp() throws Exception {
        clazz = Class.forName("org.apache.rocketmq.store.DefaultMessageStore$CleanCommitLogService");

        MessageStoreConfig messageStoreConfig = new MessageStoreConfig();
        messageStore = new DefaultMessageStore(
            messageStoreConfig, null, null, new BrokerConfig());

        Field cleanCommitLogServiceField = messageStore.getClass().getDeclaredField("cleanCommitLogService");
        cleanCommitLogServiceField.setAccessible(true);

        cleanCommitLogService = spy((DefaultMessageStore.CleanCommitLogService) (cleanCommitLogServiceField.get(messageStore)));
    }

    @After
    public void tearDown() throws Exception {
        messageStore.shutdown();
    }

    @Test
    public void isSpaceToDeleteNoCommitLog() throws Exception {
        Method m = clazz.getDeclaredMethod("isSpaceToDelete");
        m.setAccessible(true);

        Assert.assertFalse((boolean) m.invoke(cleanCommitLogService));
    }

    @Test
    public void isSpaceToDeleteWithCommitLogNoQueue() throws Exception {
        messageStore.start();
        messageStore.load();

        Method m = clazz.getDeclaredMethod("isSpaceToDelete");
        m.setAccessible(true);

        Assert.assertFalse((boolean) m.invoke(cleanCommitLogService));
    }

    @Test
    public void isSpaceToDeleteWithCommitLogDiskFull() throws Exception {
        messageStore.start();
        messageStore.load();

        Method m = clazz.getDeclaredMethod("isSpaceToDelete");
        m.setAccessible(true);

        when(cleanCommitLogService.getDiskUsageRatio()).thenReturn(0.9);
        Assert.assertTrue((boolean) m.invoke(cleanCommitLogService));
    }

    @Test
    public void isSpaceToDeleteWithCommitLogQueueFull() throws Exception {
        messageStore.start();
        messageStore.load();

        Method m = clazz.getDeclaredMethod("isSpaceToDelete");
        m.setAccessible(true);

        when(cleanCommitLogService.getDiskUsageRatio()).thenReturn(0.1);
        when(cleanCommitLogService.getQueueSpace()).thenReturn(0.9);
        Assert.assertTrue((boolean) m.invoke(cleanCommitLogService));
    }
}
