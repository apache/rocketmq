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

package org.apache.rocketmq.container;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.remoting.netty.NettyClientConfig;
import org.apache.rocketmq.remoting.netty.NettyServerConfig;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.assertj.core.util.Arrays;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static org.apache.rocketmq.container.BrokerContainerStartup.parseCmdLineToConfig;
import static org.assertj.core.api.Java6Assertions.assertThat;

@RunWith(MockitoJUnitRunner.class)
public class BrokerContainerStartupTest {
    private static final List<File> TMP_FILE_LIST = new ArrayList<>();
    private static final String BROKER_NAME_PREFIX = "TestBroker";
    private static final String SHARED_BROKER_NAME_PREFIX = "TestBrokerContainer";
    private static String brokerConfigPath;
    private static String brokerContainerConfigPath;

    @Mock
    private BrokerConfig brokerConfig;
    private String storePathRootDir = "store/test";
    @Mock
    private NettyClientConfig nettyClientConfig;
    @Mock
    private NettyServerConfig nettyServerConfig;

    @Before
    public void init() throws IOException {
        String brokerName = BROKER_NAME_PREFIX + "_" + System.currentTimeMillis();
        BrokerConfig brokerConfig = new BrokerConfig();
        brokerConfig.setBrokerName(brokerName);
        if (brokerConfig.getRocketmqHome() == null) {
            brokerConfig.setRocketmqHome("../distribution");
        }
        MessageStoreConfig storeConfig = new MessageStoreConfig();
        String baseDir = createBaseDir(brokerConfig.getBrokerName() + "_" + brokerConfig.getBrokerId()).getAbsolutePath();
        storeConfig.setStorePathRootDir(baseDir);
        storeConfig.setStorePathCommitLog(baseDir + File.separator + "commitlog");

        brokerConfigPath = "/tmp/" + brokerName;
        brokerConfig.setBrokerConfigPath(brokerConfigPath);
        File file = new File(brokerConfigPath);
        TMP_FILE_LIST.add(file);
        Properties brokerConfigProp = MixAll.object2Properties(brokerConfig);
        Properties storeConfigProp = MixAll.object2Properties(storeConfig);

        for (Object key : storeConfigProp.keySet()) {
            brokerConfigProp.put(key, storeConfigProp.get(key));
        }
        MixAll.string2File(MixAll.properties2String(brokerConfigProp), brokerConfigPath);

        brokerContainerConfigPath = "/tmp/" + SHARED_BROKER_NAME_PREFIX + System.currentTimeMillis();
        BrokerContainerConfig brokerContainerConfig = new BrokerContainerConfig();
        brokerContainerConfig.setBrokerConfigPaths(brokerConfigPath);
        if (brokerContainerConfig.getRocketmqHome() == null) {
            brokerContainerConfig.setRocketmqHome("../distribution");
        }
        File file1 = new File(brokerContainerConfigPath);
        TMP_FILE_LIST.add(file1);
        Properties brokerContainerConfigProp = MixAll.object2Properties(brokerContainerConfig);
        MixAll.string2File(MixAll.properties2String(brokerContainerConfigProp), brokerContainerConfigPath);
    }

    @After
    public void destroy() {
        for (File file : TMP_FILE_LIST) {
            UtilAll.deleteFile(file);
        }
    }

    @Test
    public void testStartBrokerContainer() {
        final BrokerContainerConfig containerConfig = new BrokerContainerConfig();
        final NettyServerConfig nettyServerConfig = new NettyServerConfig();
        final NettyClientConfig nettyClientConfig = new NettyClientConfig();
        parseCmdLineToConfig(Arrays.array("-c", brokerContainerConfigPath), containerConfig, nettyServerConfig, nettyClientConfig);
        BrokerContainer brokerContainer = BrokerContainerStartup.startBrokerContainer(
            BrokerContainerStartup.createBrokerContainer(containerConfig, nettyServerConfig, nettyClientConfig));
        assertThat(brokerContainer).isNotNull();
        List<BrokerController> brokers = BrokerContainerStartup.createAndStartBrokers(brokerContainer);
        assertThat(brokers.size()).isEqualTo(1);

        brokerContainer.shutdown();
        assertThat(brokerContainer.getBrokerControllers().size()).isEqualTo(0);
    }

    private static File createBaseDir(String prefix) {
        final File file;
        try {
            file = Files.createTempDirectory(prefix).toFile();
            TMP_FILE_LIST.add(file);
            System.out.printf("create file at %s%n", file.getAbsolutePath());
            return file;
        } catch (IOException e) {
            throw new RuntimeException("Couldn't create tmp folder", e);
        }
    }

    @Before
    public void clear() {
        UtilAll.deleteFile(new File(storePathRootDir));
    }

    @After
    public void tearDown() {
        File configFile = new File(storePathRootDir);
        UtilAll.deleteFile(configFile);
        UtilAll.deleteEmptyDirectory(configFile);
        UtilAll.deleteEmptyDirectory(configFile.getParentFile());
    }
}