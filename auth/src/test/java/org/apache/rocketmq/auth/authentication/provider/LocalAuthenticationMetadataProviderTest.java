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
package org.apache.rocketmq.auth.authentication.provider;

import org.apache.rocketmq.auth.config.AuthConfig;
import org.apache.rocketmq.auth.helper.AuthTestHelper;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class LocalAuthenticationMetadataProviderTest {

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    @Test
    public void testShutdownReleasesCacheExecutor() throws Exception {
        AuthConfig authConfig = AuthTestHelper.createDefaultConfig();
        authConfig.setAuthConfigPath(tempFolder.newFolder("auth-test").getAbsolutePath());

        LocalAuthenticationMetadataProvider provider = new LocalAuthenticationMetadataProvider();
        // Initialize provider to create the internal cache refresh executor
        provider.initialize(authConfig, () -> null);

        // After initialization, the executor should exist and not be shutdown
        Assert.assertNotNull(provider.cacheRefreshExecutor);
        Assert.assertFalse(provider.cacheRefreshExecutor.isShutdown());

        // Shutdown provider should also shutdown its executor to release resources
        provider.shutdown();

        // Verify that the cache refresh executor has been shutdown
        Assert.assertTrue(provider.cacheRefreshExecutor.isShutdown());
    }
}
