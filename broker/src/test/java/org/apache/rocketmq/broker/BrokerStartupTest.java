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

package org.apache.rocketmq.broker;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Properties;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.utils.NetworkUtil;
import org.apache.rocketmq.logging.ch.qos.logback.classic.LoggerContext;
import org.apache.rocketmq.logging.ch.qos.logback.classic.spi.ILoggingEvent;
import org.apache.rocketmq.logging.ch.qos.logback.core.read.ListAppender;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.netty.NettyServerConfig;
import org.junit.Assert;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertTrue;

public class BrokerStartupTest {

    @Test
    public void testBuildBrokerController() throws Exception {
        ListAppender<ILoggingEvent> listAppender = new ListAppender<>();
        LoggerContext context = (LoggerContext) LoggerFactory.getILoggerFactory();
        // using ListAppender to catch BrokerConsoleLogger's output.
        context.getLogger(LoggerName.BROKER_CONSOLE_NAME).addAppender(listAppender);
        listAppender.start();

        String invalidBrokerConf = "/tmp/invalid-broker.conf";
        Properties properties = new Properties();
        properties.put("namesrvAddr", "127.0.0.1:9876");
        properties.put("rocketmqHome", "../distribution");
        properties.put("brokerName", "broker-a");
        properties.put("serverWorkerThreads", "20");
        // invalid property
        properties.put("autoCreateTopicEnabled", "false");
        properties.put("brokerIp1", "127.0.0.1:10109");
        properties.put("serverSelectorThread", "1");
        MixAll.string2File(MixAll.properties2String(properties), invalidBrokerConf);

        BrokerController controller = BrokerStartup.buildBrokerController(new String[] {"-c", invalidBrokerConf});

        BrokerConfig brokerConfig = controller.getBrokerConfig();
        NettyServerConfig nettyServerConfig = controller.getNettyServerConfig();
        // assert equals to default value, because brokerConfig file set an incorrect property name.
        assertThat(brokerConfig.getBrokerIP1()).isEqualTo(NetworkUtil.getLocalAddress());
        assertThat(brokerConfig.isAutoCreateTopicEnable()).isEqualTo(true);
        assertThat(nettyServerConfig.getServerSelectorThreads()).isEqualTo(3);

        // assert equals to setting value by config file
        assertThat(brokerConfig.getNamesrvAddr()).isEqualTo("127.0.0.1:9876");
        assertThat(brokerConfig.getBrokerName()).isEqualTo("broker-a");
        assertThat(nettyServerConfig.getServerWorkerThreads()).isEqualTo(20);
        assertThat(nettyServerConfig.getServerWorkerThreads()).isEqualTo(20);

        // assert BrokerConsoleLogger has warning log output.
        List<ILoggingEvent> list = listAppender.list;
        list.forEach(log -> {
            assertTrue(log.toString().contains("brokerIp1"));
            assertTrue(log.toString().contains("autoCreateTopicEnabled"));
            assertTrue(log.toString().contains("serverSelectorThread"));
        });
    }

    @Test
    public void testProperties2SystemEnv() throws NoSuchMethodException, InvocationTargetException,
        IllegalAccessException {
        Properties properties = new Properties();
        Class<BrokerStartup> clazz = BrokerStartup.class;
        Method method = clazz.getDeclaredMethod("properties2SystemEnv", Properties.class);
        method.setAccessible(true);
        {
            properties.put("rmqAddressServerDomain", "value1");
            properties.put("rmqAddressServerSubGroup", "value2");
            method.invoke(null, properties);
            Assert.assertEquals("value1", System.getProperty("rocketmq.namesrv.domain"));
            Assert.assertEquals("value2", System.getProperty("rocketmq.namesrv.domain.subgroup"));
        }
        {
            properties.put("rmqAddressServerDomain", MixAll.WS_DOMAIN_NAME);
            properties.put("rmqAddressServerSubGroup", MixAll.WS_DOMAIN_SUBGROUP);
            method.invoke(null, properties);
            Assert.assertEquals(MixAll.WS_DOMAIN_NAME, System.getProperty("rocketmq.namesrv.domain"));
            Assert.assertEquals(MixAll.WS_DOMAIN_SUBGROUP, System.getProperty("rocketmq.namesrv.domain.subgroup"));
        }


    }
}