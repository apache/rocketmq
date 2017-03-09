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

import org.apache.rocketmq.broker.exception.BrokerException;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.MessageStore;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

@RunWith(MockitoJUnitRunner.class)
public class MessageStoreFactoryTest {
    @Mock
    private DefaultMessageStore messageStore;

    @Test
    public void buildWithoutPluginConfigured() throws Exception {
        MessageStorePluginContext ctx = new MessageStorePluginContext(new MessageStoreConfig(), null, null, new BrokerConfig());
        assertThat(MessageStoreFactory.build(ctx, messageStore)).isInstanceOf(DefaultMessageStore.class);
    }

    @Test
    public void buildWithPluginMisConfigured() throws Exception {
        BrokerConfig brokerConfig = new BrokerConfig();
        brokerConfig.setMessageStorePlugIn("NonExistentPlugin ");
        MessageStorePluginContext ctx = new MessageStorePluginContext(new MessageStoreConfig(), null, null, brokerConfig);
        try {
            MessageStoreFactory.build(ctx, messageStore);
            fail("No exception!");
        } catch (Exception e) {
            assertThat(e).isInstanceOf(BrokerException.class);
        }
    }

    @Test
    public void buildWithPluginConfigured() throws Exception {
        BrokerConfig brokerConfig = new BrokerConfig();
        brokerConfig.setMessageStorePlugIn("org.apache.rocketmq.broker.plugin.MessageStoreFactoryTest$PluginStoreMock ");
        MessageStorePluginContext ctx = new MessageStorePluginContext(new MessageStoreConfig(), null, null, brokerConfig);
        assertThat(MessageStoreFactory.build(ctx, messageStore)).isInstanceOf(PluginStoreMock.class);
    }

    /**
     * Mock.
     */
    static class PluginStoreMock extends AbstractPluginMessageStore {

        public PluginStoreMock(MessageStorePluginContext context, MessageStore next) {
            super(context, next);
        }
    }
}
