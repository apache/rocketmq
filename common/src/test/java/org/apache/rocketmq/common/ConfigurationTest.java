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
package org.apache.rocketmq.common;

import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.junit.Assert;
import org.junit.Test;

import java.util.Properties;

/**
 * @author huhui
 * @create 2019-02-22 20:57
 */
public class ConfigurationTest {
    private static final InternalLogger log = InternalLoggerFactory.getLogger("ConfigurationTest");

    @Test
    public void registerConfig() {
        Configuration configuration = new Configuration(log);
        configuration.registerConfig(new CustomeConfig("cutomer", "hello"));
    }

    @Test
    public void registerConfig1() {
        CustomeConfig customeConfig = new CustomeConfig();
        customeConfig.setConfigPath("C://");
        Configuration configuration = new Configuration(log, customeConfig);
        Properties properties = new Properties();
        properties.setProperty("date", "2019-03-01");
        properties.setProperty("auth", "yuanhu");
        configuration.registerConfig(properties);
    }

    @Test
    public void setStorePathFromConfig() {
        CustomeConfig customeConfig = new CustomeConfig();
        customeConfig.setConfigPath("C://");
        Configuration configuration = new Configuration(log, new TestConfig("jim", "19"));
        configuration.setStorePathFromConfig(customeConfig, "configPath");
    }

    @Test
    public void setStorePath() {
        Configuration configuration = new Configuration(log, new TestConfig("jim", "19"));
        configuration.setStorePath("C://");
    }

    @Test
    public void update() {
        Configuration configuration =
            new Configuration(log, "C://", new TestConfig("jim", "19"), new CustomeConfig("custome", "E://"));
        Properties pro = new Properties();
        pro.setProperty("testName", "jack");
        configuration.update(pro);
        Assert.assertTrue(configuration.getAllConfigsFormatString().contains("jack"));
    }

    @Test
    public void persist() {
        CustomeConfig customeConfig = new CustomeConfig();
        customeConfig.setConfigPath("C://");
        Configuration configuration = new Configuration(log, new TestConfig("jim", "19"));
        configuration.setStorePathFromConfig(customeConfig, "configPath");
        configuration.persist();
    }

    @Test
    public void getAllConfigsFormatString() {
        Configuration configuration = new Configuration(log, new TestConfig("jim", "19"));
        Assert.assertTrue(configuration.getAllConfigsFormatString().equals("testName=jim\n" +
                "age=19\n"));
    }

    @Test
    public void getDataVersionJson() {
        Configuration configuration = new Configuration(log, new TestConfig("jim", "19"));
        configuration.getDataVersionJson();
    }

    @Test
    public void getAllConfigs() {
        CustomeConfig customeConfig = new CustomeConfig();
        customeConfig.setConfigPath("E://");
        customeConfig.setName("tom");
        Configuration configuration = new Configuration(log,customeConfig);
        Properties allConfigs = configuration.getAllConfigs();
        Assert.assertTrue(allConfigs.getProperty("configPath").equals("E://"));
        Assert.assertTrue(allConfigs.getProperty("name").equals("tom"));
    }

    class CustomeConfig {

        String name;
        String configPath = "/etc/test";

        public CustomeConfig() {}

        public CustomeConfig(String name, String configPath) {
            this.name = name;
            this.configPath = configPath;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getConfigPath() {
            return configPath;
        }

        public void setConfigPath(String configPath) {
            this.configPath = configPath;
        }
    }

    class TestConfig {
        String testName;
        String age;

        public TestConfig(String testName, String age) {
            this.testName = testName;
            this.age = age;
        }

        public String getTestName() {

            return testName;
        }

        public void setTestName(String testName) {
            this.testName = testName;
        }

        public String getAge() {
            return age;
        }

        public void setAge(String age) {
            this.age = age;
        }
    }

}