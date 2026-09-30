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

package org.apache.rocketmq.store.plugin;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.fail;

import java.lang.reflect.InvocationTargetException;
import java.util.concurrent.CompletableFuture;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.store.MessageStore;
import org.apache.rocketmq.store.QueryMessageResult;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.apache.rocketmq.store.rocksdb.MessageRocksDBStorage;
import org.apache.rocketmq.store.timer.rocksdb.TimerMessageRocksDBStore;
import org.apache.rocketmq.store.transaction.TransMessageRocksDBStore;
import org.junit.Test;

public class MessageStoreFactoryTest {

    private static final String MISSING_PLUGIN_CLASS =
        "org.apache.rocketmq.store.plugin.NoSuchPluginClassSurelyMissing";
    private static final String THROWING_PLUGIN =
        MessageStoreFactoryTest.class.getName() + "$ThrowingPlugin";
    private static final String OUTER_PLUGIN =
        MessageStoreFactoryTest.class.getName() + "$OuterPlugin";
    private static final String INNER_PLUGIN =
        MessageStoreFactoryTest.class.getName() + "$InnerPlugin";

    private MessageStorePluginContext buildContext(String plugin) {
        BrokerConfig brokerConfig = new BrokerConfig();
        brokerConfig.setMessageStorePlugIn(plugin);
        return new MessageStorePluginContext(new MessageStoreConfig(), null, null, brokerConfig, null);
    }

    @Test
    public void testPluginClassNotExist() throws Exception {
        MessageStorePluginContext context = buildContext(MISSING_PLUGIN_CLASS);
        try {
            MessageStoreFactory.build(context, null);
            fail("build should fail when the plugin class does not exist");
        } catch (RuntimeException e) {
            assertThat(e.getMessage())
                .contains("Failed to initialize message store plugin")
                .contains("NoSuchPluginClassSurelyMissing");
            assertThat(e.getCause()).isInstanceOf(ClassNotFoundException.class);
        }
    }

    @Test
    public void testPluginConstructorThrows() throws Exception {
        MessageStorePluginContext context = buildContext(THROWING_PLUGIN);
        try {
            MessageStoreFactory.build(context, null);
            fail("build should fail when the plugin constructor throws");
        } catch (RuntimeException e) {
            assertThat(e.getMessage())
                .contains("Failed to initialize message store plugin")
                .contains(THROWING_PLUGIN)
                .doesNotContain("not found");
            assertThat(e.getCause()).isInstanceOf(InvocationTargetException.class);
            Throwable rootCause = e.getCause().getCause();
            assertThat(rootCause).isInstanceOf(IllegalStateException.class);
            assertThat(rootCause).hasMessage("boom");
        }
    }

    @Test
    public void testBuildSinglePlugin() throws Exception {
        MessageStorePluginContext context = buildContext(INNER_PLUGIN);
        MessageStore store = MessageStoreFactory.build(context, null);
        assertThat(store).isInstanceOf(InnerPlugin.class);
        assertThat(((InnerPlugin) store).getNext()).isNull();
        assertThat(((InnerPlugin) store).context).isSameAs(context);
    }

    @Test
    public void testBuildChainsPluginsInConfigOrder() throws Exception {
        // no space after the comma: build splits the config on "," without trimming entries
        MessageStorePluginContext context = buildContext(OUTER_PLUGIN + "," + INNER_PLUGIN);
        MessageStore store = MessageStoreFactory.build(context, null);
        assertThat(store).isInstanceOf(OuterPlugin.class);
        AbstractPluginMessageStore outer = (AbstractPluginMessageStore) store;
        assertThat(outer.getNext()).isInstanceOf(InnerPlugin.class);
        AbstractPluginMessageStore inner = (AbstractPluginMessageStore) outer.getNext();
        assertThat(inner.getNext()).isNull();
        assertThat(outer.context).isSameAs(context);
        assertThat(inner.context).isSameAs(context);
    }

    @Test
    public void testNullPluginConfigReturnsOriginalStore() throws Exception {
        MessageStorePluginContext context = buildContext(null);
        MessageStore delegate = new InnerPlugin(context, null);
        assertThat(MessageStoreFactory.build(context, delegate)).isSameAs(delegate);
    }

    @Test
    public void testBlankPluginConfigReturnsOriginalStore() throws Exception {
        MessageStorePluginContext context = buildContext("   ");
        MessageStore delegate = new InnerPlugin(context, null);
        assertThat(MessageStoreFactory.build(context, delegate)).isSameAs(delegate);
    }

    /**
     * Test-local plugin base. AbstractPluginMessageStore leaves a few MessageStore methods
     * unimplemented in this branch, so concrete plugins used by the tests fill the gap here.
     */
    private static class TestPlugin extends AbstractPluginMessageStore {
        TestPlugin(MessageStorePluginContext context, MessageStore next) {
            super(context, next);
        }

        @Override
        public QueryMessageResult queryMessage(String topic, String key, int maxNum, long begin, long end,
            String indexType, String lastKey) {
            return next.queryMessage(topic, key, maxNum, begin, end, indexType, lastKey);
        }

        @Override
        public CompletableFuture<QueryMessageResult> queryMessageAsync(String topic, String key, int maxNum,
            long begin, long end, String indexType, String lastKey) {
            return next.queryMessageAsync(topic, key, maxNum, begin, end, indexType, lastKey);
        }

        @Override
        public MessageRocksDBStorage getMessageRocksDBStorage() {
            return next == null ? null : next.getMessageRocksDBStorage();
        }

        @Override
        public TimerMessageRocksDBStore getTimerMessageRocksDBStore() {
            return next == null ? null : next.getTimerMessageRocksDBStore();
        }

        @Override
        public TransMessageRocksDBStore getTransMessageRocksDBStore() {
            return next == null ? null : next.getTransMessageRocksDBStore();
        }

        @Override
        public void setTimerMessageRocksDBStore(TimerMessageRocksDBStore timerMessageRocksDBStore) {
            next.setTimerMessageRocksDBStore(timerMessageRocksDBStore);
        }

        @Override
        public void setTransMessageRocksDBStore(TransMessageRocksDBStore transMessageRocksDBStore) {
            next.setTransMessageRocksDBStore(transMessageRocksDBStore);
        }
    }

    public static class OuterPlugin extends TestPlugin {
        public OuterPlugin(MessageStorePluginContext context, MessageStore next) {
            super(context, next);
        }
    }

    public static class InnerPlugin extends TestPlugin {
        public InnerPlugin(MessageStorePluginContext context, MessageStore next) {
            super(context, next);
        }
    }

    public static class ThrowingPlugin extends TestPlugin {
        public ThrowingPlugin(MessageStorePluginContext context, MessageStore next) {
            super(context, next);
            throw new IllegalStateException("boom");
        }
    }
}
