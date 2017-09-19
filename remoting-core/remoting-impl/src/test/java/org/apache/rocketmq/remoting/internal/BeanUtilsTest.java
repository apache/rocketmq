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

package org.apache.rocketmq.remoting.internal;

import java.util.Properties;
import org.apache.rocketmq.remoting.config.RemotingConfig;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class BeanUtilsTest {
    private Properties properties = new Properties();
    private final static String CLIENT_CONFIG_PREFIX = "client.";
    private final static String SERVER_CONFIG_PREFIX = "server.";
    private final static String TCP_CONFIG_PREFIX = "tcp.";
    private final static String CONNECTION_CHANNEL_IDLE_SECONDS = "connection.channel.idle.seconds";
    private final static String CLIENT_ASYNC_CALLBACK_EXECUTOR_THREADS = CLIENT_CONFIG_PREFIX + "async.callback.executor.threads";
    private final static String CLIENT_POOLEDBYTEBUF_ALLOCATOR_ENABLE = CLIENT_CONFIG_PREFIX + "pooled.bytebuf.allocator.enable";
    private final static String SERVER_LISTEN_PORT = SERVER_CONFIG_PREFIX + "listen.port";
    private final static String TCP_SO_REUSE_ADDRESS = TCP_CONFIG_PREFIX + "so.reuse.address";
    private final static String TCP_SO_KEEPALIVE = TCP_CONFIG_PREFIX + "so.keep.alive";
    private final static String TCP_SO_NODELAY = TCP_CONFIG_PREFIX + "so.no.delay";
    private final static String TCP_SO_SNDBUF_SIZE = TCP_CONFIG_PREFIX + "so.snd.buf.size";
    private final static String TCP_SO_RCVBUF_SIZE = TCP_CONFIG_PREFIX + "so.rcv.buf.size";
    private final static String TCP_SO_BACKLOG_SIZE = TCP_CONFIG_PREFIX + "so.backlog.size";
    private final static String TCP_SO_TIMEOUT = TCP_CONFIG_PREFIX + "so.timeout";
    private final static String TCP_SO_LINGER = TCP_CONFIG_PREFIX + "so.linger";

    @Before
    public void init() {
        properties.put(CONNECTION_CHANNEL_IDLE_SECONDS, 3);

        properties.put(CLIENT_ASYNC_CALLBACK_EXECUTOR_THREADS, 10);
        properties.put(CLIENT_POOLEDBYTEBUF_ALLOCATOR_ENABLE, true);

        properties.put(SERVER_LISTEN_PORT, 900);

        properties.put(TCP_SO_REUSE_ADDRESS, false);
        properties.put(TCP_SO_KEEPALIVE, false);
        properties.put(TCP_SO_NODELAY, false);
        properties.put(TCP_SO_SNDBUF_SIZE, 100);
        properties.put(TCP_SO_RCVBUF_SIZE, 100);
        properties.put(TCP_SO_BACKLOG_SIZE, 100);
        properties.put(TCP_SO_TIMEOUT, 5000);
        properties.put(TCP_SO_LINGER, 100);

        properties.put(ClientConfig.STRING_TEST, "kaka");
    }

    @Test
    public void populateTest() {
        ClientConfig config = BeanUtils.populate(properties, ClientConfig.class);

        //RemotingConfig config = BeanUtils.populate(properties, RemotingConfig.class);
        Assert.assertEquals(config.getConnectionChannelIdleSeconds(), 3);

        Assert.assertEquals(config.getClientAsyncCallbackExecutorThreads(), 10);
        Assert.assertTrue(config.isClientPooledBytebufAllocatorEnable());

        Assert.assertEquals(config.getServerListenPort(), 900);

        Assert.assertFalse(config.isTcpSoReuseAddress());

        Assert.assertFalse(config.isTcpSoKeepAlive());
        Assert.assertFalse(config.isTcpSoNoDelay());
        Assert.assertEquals(config.getTcpSoSndBufSize(), 100);
        Assert.assertEquals(config.getTcpSoRcvBufSize(), 100);
        Assert.assertEquals(config.getTcpSoBacklogSize(), 100);
        Assert.assertEquals(config.getTcpSoTimeout(), 5000);
        Assert.assertEquals(config.getTcpSoLinger(), 100);

        Assert.assertEquals(config.getStringTest(), "kaka");
    }

    @Test
    public void populateExistObj() {
        ClientConfig config = new ClientConfig();
        config.setServerListenPort(8118);

        Assert.assertEquals(config.getServerListenPort(), 8118);

        config = BeanUtils.populate(properties, config);

        Assert.assertEquals(config.getConnectionChannelIdleSeconds(), 3);

        Assert.assertEquals(config.getClientAsyncCallbackExecutorThreads(), 10);
        Assert.assertTrue(config.isClientPooledBytebufAllocatorEnable());

        Assert.assertEquals(config.getServerListenPort(), 900);

        Assert.assertFalse(config.isTcpSoReuseAddress());
        Assert.assertFalse(config.isTcpSoKeepAlive());
        Assert.assertFalse(config.isTcpSoNoDelay());
        Assert.assertEquals(config.getTcpSoSndBufSize(), 100);
        Assert.assertEquals(config.getTcpSoRcvBufSize(), 100);
        Assert.assertEquals(config.getTcpSoBacklogSize(), 100);
        Assert.assertEquals(config.getTcpSoTimeout(), 5000);
        Assert.assertEquals(config.getTcpSoLinger(), 100);

        Assert.assertEquals(config.getStringTest(), "kaka");
    }

    public static class ClientConfig extends RemotingConfig {
        public final static String STRING_TEST = "string.test";
        String stringTest = "foobar";

        public ClientConfig() {
        }

        public String getStringTest() {
            return stringTest;
        }

        public void setStringTest(String stringTest) {
            this.stringTest = stringTest;
        }
    }

}