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
import org.apache.rocketmq.proxy.config.ConfigurationManager;
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

    private File defaultCertFile;
    private File defaultKeyFile;
    private File firstCertFile;
    private File firstKeyFile;
    private File secondCertFile;
    private File secondKeyFile;

    @Before
    public void setUp() throws Exception {
        TlsSniManager.resetInstance();
        ConfigurationManager.initEnv();
        ConfigurationManager.initConfig();
        ConfigurationManager.getProxyConfig().setTlsTestModeEnable(true);

        defaultCertFile = tempDir.newFile("default.crt");
        defaultKeyFile = tempDir.newFile("default.key");
        try (FileWriter w = new FileWriter(defaultCertFile)) { w.write("default cert"); }
        try (FileWriter w = new FileWriter(defaultKeyFile)) { w.write("default key"); }

        firstCertFile = tempDir.newFile("example.crt");
        firstKeyFile = tempDir.newFile("example.key");
        try (FileWriter w = new FileWriter(firstCertFile)) { w.write("example cert"); }
        try (FileWriter w = new FileWriter(firstKeyFile)) { w.write("example key"); }

        secondCertFile = tempDir.newFile("sample.crt");
        secondKeyFile = tempDir.newFile("sample.key");
        try (FileWriter w = new FileWriter(secondCertFile)) { w.write("sample cert"); }
        try (FileWriter w = new FileWriter(secondKeyFile)) { w.write("sample key"); }
    }

    @After
    public void tearDown() throws Exception {
        TlsSniManager.resetInstance();
    }

    @Test
    public void testInitializeWithTestMode() {
        TlsSniManager sniManager = new TlsSniManager();
        sniManager.initialize(createTestModeProxyConfig());

        assertNotNull(sniManager.getDefaultContext());
        assertNotNull(sniManager.getSslContext("test.example.com"));
    }

    @Test
    public void testWildcardMatch_FirstDomain() {
        TlsSniManager sniManager = new TlsSniManager();
        sniManager.initialize(createTestModeProxyConfig());

        SslContext ctx = sniManager.getSslContext("foo.example.com");
        assertNotNull(ctx);
    }

    @Test
    public void testWildcardMatch_SecondDomain() {
        TlsSniManager sniManager = new TlsSniManager();
        sniManager.initialize(createTestModeProxyConfig());

        SslContext ctx = sniManager.getSslContext("mq.sample.org");
        assertNotNull(ctx);
    }

    @Test
    public void testExactMatch() {
        TlsSniManager sniManager = new TlsSniManager();
        sniManager.initialize(createTestModeProxyConfig());

        SslContext ctx = sniManager.getSslContext("example.com");
        assertNotNull(ctx);
    }

    @Test
    public void testNoMatchFallbackToDefault() {
        TlsSniManager sniManager = new TlsSniManager();
        sniManager.initialize(createTestModeProxyConfig());

        SslContext ctx = sniManager.getSslContext("unknown.other.com");
        assertNotNull(ctx);
        assertSame(sniManager.getDefaultContext(), ctx);
    }

    @Test
    public void testNullSniFallbackToDefault() {
        TlsSniManager sniManager = new TlsSniManager();
        sniManager.initialize(createTestModeProxyConfig());

        SslContext ctx = sniManager.getSslContext(null);
        assertNotNull(ctx);
        assertSame(sniManager.getDefaultContext(), ctx);
    }

    @Test
    public void testEmptySniFallbackToDefault() {
        TlsSniManager sniManager = new TlsSniManager();
        sniManager.initialize(createTestModeProxyConfig());

        SslContext ctx = sniManager.getSslContext("");
        assertNotNull(ctx);
        assertSame(sniManager.getDefaultContext(), ctx);
    }

    @Test
    public void testMultiLevelSubdomainNoMatchByDefault() {
        TlsSniManager sniManager = new TlsSniManager();
        sniManager.initialize(createTestModeProxyConfig());

        // a.b.example.com should NOT match *.example.com by default
        SslContext ctx = sniManager.getSslContext("a.b.example.com");
        assertNotNull(ctx);
        assertSame(sniManager.getDefaultContext(), ctx);
    }

    @Test
    public void testMultiLevelSubdomainMatchWhenEnabled() {
        TlsSniManager sniManager = new TlsSniManager();
        sniManager.initialize(createTestModeProxyConfigWithMultiLevel());

        // a.b.example.com SHOULD match *.example.com when tlsWildcardMatchMultiLevel is true
        SslContext ctx = sniManager.getSslContext("a.b.example.com");
        assertNotNull(ctx);
        // Should NOT be the default context
        org.junit.Assert.assertNotSame(sniManager.getDefaultContext(), ctx);
    }

    @Test
    public void testReloadDomainContext() {
        TlsSniManager sniManager = new TlsSniManager();
        sniManager.initialize(createTestModeProxyConfig());

        SslContext before = sniManager.getSslContext("foo.example.com");
        sniManager.reloadDomainContext("*.example.com");
        SslContext after = sniManager.getSslContext("foo.example.com");
        assertNotNull(before);
        assertNotNull(after);
    }

    @Test
    public void testReloadDefaultContext() {
        TlsSniManager sniManager = new TlsSniManager();
        sniManager.initialize(createTestModeProxyConfig());

        SslContext before = sniManager.getDefaultContext();
        sniManager.reloadDefaultContext();
        SslContext after = sniManager.getDefaultContext();
        assertNotNull(before);
        assertNotNull(after);
    }

    @Test
    public void testDomainConfigsNotEmpty() {
        TlsSniManager sniManager = new TlsSniManager();
        sniManager.initialize(createTestModeProxyConfig());

        Map<String, TlsDomainConfig> configs = sniManager.getDomainConfigs();
        assertNotNull(configs);
        assertEquals(2, configs.size());
        assertTrue(configs.containsKey("*.example.com"));
        assertTrue(configs.containsKey("*.sample.org"));
    }

    @Test
    public void testSingleton() {
        TlsSniManager s1 = TlsSniManager.getInstance();
        TlsSniManager s2 = TlsSniManager.getInstance();
        assertSame(s1, s2);
    }

    @Test
    public void testWildcardMatchOrderDeterministic() {
        // Longer wildcard should match before shorter wildcard
        TlsSniManager sniManager = new TlsSniManager();
        sniManager.initialize(createTestModeProxyConfigWithOverlappingWildcards());

        // foo.sub.example.com should match *.sub.example.com (longer), not *.example.com (shorter)
        SslContext ctx = sniManager.getSslContext("foo.sub.example.com");
        assertNotNull(ctx);
        // Verify it's NOT the default context
        org.junit.Assert.assertNotSame(sniManager.getDefaultContext(), ctx);
    }

    private org.apache.rocketmq.proxy.config.ProxyConfig createTestModeProxyConfig() {
        org.apache.rocketmq.proxy.config.ProxyConfig config = new org.apache.rocketmq.proxy.config.ProxyConfig();
        config.setTlsTestModeEnable(true);

        Map<String, TlsDomainConfig> domainConfigs = new HashMap<>();
        TlsDomainConfig firstConfig = new TlsDomainConfig();
        firstConfig.setCertPath(firstCertFile.getAbsolutePath());
        firstConfig.setKeyPath(firstKeyFile.getAbsolutePath());
        domainConfigs.put("*.example.com", firstConfig);

        TlsDomainConfig secondConfig = new TlsDomainConfig();
        secondConfig.setCertPath(secondCertFile.getAbsolutePath());
        secondConfig.setKeyPath(secondKeyFile.getAbsolutePath());
        domainConfigs.put("*.sample.org", secondConfig);

        config.setTlsDomainConfigs(domainConfigs);

        return config;
    }

    private org.apache.rocketmq.proxy.config.ProxyConfig createTestModeProxyConfigWithMultiLevel() {
        org.apache.rocketmq.proxy.config.ProxyConfig config = createTestModeProxyConfig();
        config.setTlsWildcardMatchMultiLevel(true);
        return config;
    }

    private org.apache.rocketmq.proxy.config.ProxyConfig createTestModeProxyConfigWithOverlappingWildcards() {
        org.apache.rocketmq.proxy.config.ProxyConfig config = new org.apache.rocketmq.proxy.config.ProxyConfig();
        config.setTlsTestModeEnable(true);

        Map<String, TlsDomainConfig> domainConfigs = new HashMap<>();
        TlsDomainConfig broadConfig = new TlsDomainConfig();
        broadConfig.setCertPath(firstCertFile.getAbsolutePath());
        broadConfig.setKeyPath(firstKeyFile.getAbsolutePath());
        domainConfigs.put("*.example.com", broadConfig);

        TlsDomainConfig specificConfig = new TlsDomainConfig();
        specificConfig.setCertPath(secondCertFile.getAbsolutePath());
        specificConfig.setKeyPath(secondKeyFile.getAbsolutePath());
        domainConfigs.put("*.sub.example.com", specificConfig);

        config.setTlsDomainConfigs(domainConfigs);

        return config;
    }

    @Test
    public void testStdSslContext_WildcardMatch() {
        TlsSniManager sniManager = new TlsSniManager();
        sniManager.initialize(createTestModeProxyConfig());

        io.netty.handler.ssl.SslContext ctx = sniManager.getStdSslContext("foo.example.com");
        assertNotNull(ctx);
        org.junit.Assert.assertNotSame(sniManager.getStdDefaultContext(), ctx);
    }

    @Test
    public void testStdSslContext_FallbackToDefault() {
        TlsSniManager sniManager = new TlsSniManager();
        sniManager.initialize(createTestModeProxyConfig());

        io.netty.handler.ssl.SslContext ctx = sniManager.getStdSslContext("unknown.other.com");
        assertNotNull(ctx);
        assertSame(sniManager.getStdDefaultContext(), ctx);
    }

    @Test
    public void testStdDefaultContext_NotNull() {
        TlsSniManager sniManager = new TlsSniManager();
        sniManager.initialize(createTestModeProxyConfig());

        assertNotNull(sniManager.getStdDefaultContext());
    }

    @Test
    public void testReloadDomainContext_ActuallyReplaces() {
        TlsSniManager sniManager = new TlsSniManager();
        sniManager.initialize(createTestModeProxyConfig());

        SslContext before = sniManager.getSslContext("foo.example.com");
        sniManager.reloadDomainContext("*.example.com");
        SslContext after = sniManager.getSslContext("foo.example.com");

        assertNotNull(before);
        assertNotNull(after);
        org.junit.Assert.assertNotSame("reloadDomainContext should produce a new SslContext", before, after);
    }

    @Test
    public void testReloadDefaultContext_ActuallyReplaces() {
        TlsSniManager sniManager = new TlsSniManager();
        sniManager.initialize(createTestModeProxyConfig());

        SslContext before = sniManager.getDefaultContext();
        sniManager.reloadDefaultContext();
        SslContext after = sniManager.getDefaultContext();

        assertNotNull(before);
        assertNotNull(after);
        org.junit.Assert.assertNotSame("reloadDefaultContext should produce a new SslContext", before, after);
    }

    @Test
    public void testWildcardMatch_CaseInsensitive() {
        TlsSniManager sniManager = new TlsSniManager();
        sniManager.initialize(createTestModeProxyConfig());

        SslContext ctx = sniManager.getSslContext("FOO.EXAMPLE.COM");
        assertNotNull(ctx);
        org.junit.Assert.assertNotSame(sniManager.getDefaultContext(), ctx);
    }

    @Test
    public void testExactMatchTakesPriorityOverWildcard() {
        TlsSniManager sniManager = new TlsSniManager();
        sniManager.initialize(createTestModeProxyConfigWithExactAndWildcard());

        SslContext exact = sniManager.getSslContext("exact.example.com");
        SslContext wildcard = sniManager.getSslContext("other.example.com");

        assertNotNull(exact);
        assertNotNull(wildcard);
        org.junit.Assert.assertNotSame(exact, wildcard);
    }

    private org.apache.rocketmq.proxy.config.ProxyConfig createTestModeProxyConfigWithExactAndWildcard() {
        org.apache.rocketmq.proxy.config.ProxyConfig config = new org.apache.rocketmq.proxy.config.ProxyConfig();
        config.setTlsTestModeEnable(true);

        Map<String, TlsDomainConfig> domainConfigs = new HashMap<>();

        TlsDomainConfig wildcardConfig = new TlsDomainConfig();
        wildcardConfig.setCertPath(firstCertFile.getAbsolutePath());
        wildcardConfig.setKeyPath(firstKeyFile.getAbsolutePath());
        domainConfigs.put("*.example.com", wildcardConfig);

        TlsDomainConfig exactConfig = new TlsDomainConfig();
        exactConfig.setCertPath(secondCertFile.getAbsolutePath());
        exactConfig.setKeyPath(secondKeyFile.getAbsolutePath());
        domainConfigs.put("exact.example.com", exactConfig);

        config.setTlsDomainConfigs(domainConfigs);
        return config;
    }
}
