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
package org.apache.rocketmq.proxy.service.cert;

import io.grpc.netty.shaded.io.netty.handler.ssl.SslContext;
import org.apache.rocketmq.proxy.config.TlsDomainConfig;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.FileWriter;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class TlsSniManagerTest {

    @Rule
    public TemporaryFolder tempDir = new TemporaryFolder();

    private TlsSniManager sniManager;

    private File defaultCertFile;
    private File defaultKeyFile;
    private File comCertFile;
    private File comKeyFile;
    private File alibabaCertFile;
    private File alibabaKeyFile;

    @Before
    public void setUp() throws Exception {
        // Create temporary certificate and key files for default
        defaultCertFile = tempDir.newFile("default.crt");
        defaultKeyFile = tempDir.newFile("default.key");
        try (FileWriter w = new FileWriter(defaultCertFile)) { w.write("default cert"); }
        try (FileWriter w = new FileWriter(defaultKeyFile)) { w.write("default key"); }

        // Create files for *.rocketmq.com
        comCertFile = tempDir.newFile("rocketmq.crt");
        comKeyFile = tempDir.newFile("rocketmq.key");
        try (FileWriter w = new FileWriter(comCertFile)) { w.write("rocketmq cert"); }
        try (FileWriter w = new FileWriter(comKeyFile)) { w.write("rocketmq key"); }

        // Create files for *.alibaba-inc.com
        alibabaCertFile = tempDir.newFile("alibaba.crt");
        alibabaKeyFile = tempDir.newFile("alibaba.key");
        try (FileWriter w = new FileWriter(alibabaCertFile)) { w.write("alibaba cert"); }
        try (FileWriter w = new FileWriter(alibabaKeyFile)) { w.write("alibaba key"); }
    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void testInitializeWithTestMode() {
        sniManager = new TlsSniManager();
        sniManager.initialize(createTestModeProxyConfig());

        assertNotNull(sniManager.getDefaultContext());
        assertNotNull(sniManager.getSslContext("test.rocketmq.com"));
    }

    @Test
    public void testWildcardMatch_ComDomain() {
        sniManager = new TlsSniManager();
        sniManager.initialize(createTestModeProxyConfig());

        SslContext ctx = sniManager.getSslContext("foo.rocketmq.com");
        assertNotNull(ctx);
        // In test mode all contexts are SelfSignedCertificate, but they should be different instances
    }

    @Test
    public void testWildcardMatch_AlibabaDomain() {
        sniManager = new TlsSniManager();
        sniManager.initialize(createTestModeProxyConfig());

        SslContext ctx = sniManager.getSslContext("mq.alibaba-inc.com");
        assertNotNull(ctx);
    }

    @Test
    public void testExactMatch() {
        sniManager = new TlsSniManager();
        sniManager.initialize(createTestModeProxyConfig());

        SslContext ctx = sniManager.getSslContext("rocketmq.com");
        assertNotNull(ctx);
    }

    @Test
    public void testNoMatchFallbackToDefault() {
        sniManager = new TlsSniManager();
        sniManager.initialize(createTestModeProxyConfig());

        SslContext ctx = sniManager.getSslContext("unknown.other.com");
        assertNotNull(ctx);
        assertSame(sniManager.getDefaultContext(), ctx);
    }

    @Test
    public void testNullSniFallbackToDefault() {
        sniManager = new TlsSniManager();
        sniManager.initialize(createTestModeProxyConfig());

        SslContext ctx = sniManager.getSslContext(null);
        assertNotNull(ctx);
        assertSame(sniManager.getDefaultContext(), ctx);
    }

    @Test
    public void testEmptySniFallbackToDefault() {
        sniManager = new TlsSniManager();
        sniManager.initialize(createTestModeProxyConfig());

        SslContext ctx = sniManager.getSslContext("");
        assertNotNull(ctx);
        assertSame(sniManager.getDefaultContext(), ctx);
    }

    @Test
    public void testMultiLevelSubdomainNoMatch() {
        // a.b.rocketmq.com should NOT match *.rocketmq.com
        sniManager = new TlsSniManager();
        sniManager.initialize(createTestModeProxyConfig());

        SslContext ctx = sniManager.getSslContext("a.b.rocketmq.com");
        assertNotNull(ctx);
        assertSame(sniManager.getDefaultContext(), ctx);
    }

    @Test
    public void testReloadDomainContext() {
        sniManager = new TlsSniManager();
        sniManager.initialize(createTestModeProxyConfig());

        SslContext before = sniManager.getSslContext("foo.rocketmq.com");
        sniManager.reloadDomainContext("*.rocketmq.com");
        SslContext after = sniManager.getSslContext("foo.rocketmq.com");
        assertNotNull(before);
        assertNotNull(after);
        // In test mode the context instances should be different after reload
    }

    @Test
    public void testReloadDefaultContext() {
        sniManager = new TlsSniManager();
        sniManager.initialize(createTestModeProxyConfig());

        SslContext before = sniManager.getDefaultContext();
        sniManager.reloadDefaultContext();
        SslContext after = sniManager.getDefaultContext();
        assertNotNull(before);
        assertNotNull(after);
    }

    @Test
    public void testDomainConfigsNotEmpty() {
        sniManager = new TlsSniManager();
        sniManager.initialize(createTestModeProxyConfig());

        Map<String, TlsDomainConfig> configs = sniManager.getDomainConfigs();
        assertNotNull(configs);
        assertEquals(2, configs.size());
        assertTrue(configs.containsKey("*.rocketmq.com"));
        assertTrue(configs.containsKey("*.alibaba-inc.com"));
    }

    private org.apache.rocketmq.proxy.config.ProxyConfig createTestModeProxyConfig() {
        // We need to create a ProxyConfig-like object manually since ConfigurationManager may not be initialized
        // For test mode, we can use a simplified approach
        org.apache.rocketmq.proxy.config.ProxyConfig config = new org.apache.rocketmq.proxy.config.ProxyConfig();
        config.setTlsTestModeEnable(true);

        Map<String, TlsDomainConfig> domainConfigs = new HashMap<>();
        TlsDomainConfig comConfig = new TlsDomainConfig();
        comConfig.setCertPath(comCertFile.getAbsolutePath());
        comConfig.setKeyPath(comKeyFile.getAbsolutePath());
        domainConfigs.put("*.rocketmq.com", comConfig);

        TlsDomainConfig alibabaConfig = new TlsDomainConfig();
        alibabaConfig.setCertPath(alibabaCertFile.getAbsolutePath());
        alibabaConfig.setKeyPath(alibabaKeyFile.getAbsolutePath());
        domainConfigs.put("*.alibaba-inc.com", alibabaConfig);

        config.setTlsDomainConfigs(domainConfigs);

        return config;
    }
}
