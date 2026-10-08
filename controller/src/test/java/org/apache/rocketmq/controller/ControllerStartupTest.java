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
package org.apache.rocketmq.controller;

import java.io.File;
import java.io.FileOutputStream;
import java.lang.reflect.Field;
import java.util.Properties;
import org.apache.rocketmq.auth.config.AuthConfig;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class ControllerStartupTest {
    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Test
    public void loadsAuthConfigurationForController() throws Exception {
        File configFile = temporaryFolder.newFile("controller.properties");
        File controllerStore = temporaryFolder.newFolder("controller-store");
        Properties properties = new Properties();
        properties.setProperty("rocketmqHome", temporaryFolder.getRoot().getAbsolutePath());
        properties.setProperty("controllerDLegerSelfId", "n0");
        properties.setProperty("controllerDLegerGroup", "controller-cluster");
        properties.setProperty("controllerStorePath", controllerStore.getAbsolutePath());
        properties.setProperty("authenticationEnabled", "true");
        properties.setProperty("authorizationEnabled", "true");
        try (FileOutputStream outputStream = new FileOutputStream(configFile)) {
            properties.store(outputStream, null);
        }

        ControllerManager controllerManager = ControllerStartup.createControllerManager(
            new String[] {"-c", configFile.getAbsolutePath()});
        Field authConfigField = ControllerManager.class.getDeclaredField("authConfig");
        authConfigField.setAccessible(true);
        AuthConfig authConfig = (AuthConfig) authConfigField.get(controllerManager);

        Assert.assertTrue(authConfig.isAuthenticationEnabled());
        Assert.assertTrue(authConfig.isAuthorizationEnabled());
        Assert.assertEquals("controller-n0", authConfig.getConfigName());
        Assert.assertEquals("controller-cluster", authConfig.getClusterName());
        Assert.assertEquals(new File(controllerStore, "config").getAbsolutePath(), authConfig.getAuthConfigPath());
    }
}
