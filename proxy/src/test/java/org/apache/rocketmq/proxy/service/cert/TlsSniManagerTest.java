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
import io.grpc.netty.shaded.io.netty.util.Mapping;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

public class TlsSniManagerTest {

    private TlsSniManager manager;
    private SslContext defaultCtx;
    private SslContext exampleCtx;
    private SslContext sampleCtx;

    @Before
    public void setUp() {
        manager = new TlsSniManager();
        defaultCtx = mock(SslContext.class);
        exampleCtx = mock(SslContext.class);
        sampleCtx = mock(SslContext.class);

        manager.setDefaultSslContext(defaultCtx);
    }

    @Test
    public void testResolveDefault() {
        assertSame(defaultCtx, manager.resolve(null));
        assertSame(defaultCtx, manager.resolve(""));
        assertSame(defaultCtx, manager.resolve("unknown.host.com"));
    }

    @Test
    public void testResolveDomain() {
        manager.putDomainSslContext("*.example.com", exampleCtx);

        assertSame(exampleCtx, manager.resolve("foo.example.com"));
        assertSame(exampleCtx, manager.resolve("bar.example.com"));
        assertSame(defaultCtx, manager.resolve("other.org"));
    }

    @Test
    public void testResolveMultipleDomains() {
        manager.putDomainSslContext("*.example.com", exampleCtx);
        manager.putDomainSslContext("*.sample.org", sampleCtx);

        assertSame(exampleCtx, manager.resolve("foo.example.com"));
        assertSame(sampleCtx, manager.resolve("bar.sample.org"));
        assertSame(defaultCtx, manager.resolve("other.test"));
    }

    @Test
    public void testResolveBareDomain() {
        manager.putDomainSslContext("*.example.com", exampleCtx);

        assertSame(exampleCtx, manager.resolve("example.com"));
    }

    @Test
    public void testResolveCaseInsensitive() {
        manager.putDomainSslContext("*.example.com", exampleCtx);

        assertSame(exampleCtx, manager.resolve("FOO.EXAMPLE.COM"));
    }

    @Test
    public void testReloadDomain() {
        manager.putDomainSslContext("*.example.com", exampleCtx);
        SslContext newCtx = mock(SslContext.class);

        manager.reloadDomain("*.example.com", newCtx);

        assertSame(newCtx, manager.resolve("foo.example.com"));
    }

    @Test
    public void testReloadDefault() {
        SslContext newDefault = mock(SslContext.class);
        manager.reloadDefault(newDefault);

        assertSame(newDefault, manager.resolve("unknown.host"));
        assertSame(newDefault, manager.getDefaultSslContext());
    }

    @Test
    public void testAsMapping() {
        manager.putDomainSslContext("*.example.com", exampleCtx);
        Mapping<String, SslContext> mapping = manager.asMapping();

        assertSame(exampleCtx, mapping.map("foo.example.com"));
        assertSame(defaultCtx, mapping.map("other.test"));
    }

    @Test
    public void testHasDomainConfigs() {
        assertFalse(manager.hasDomainConfigs());

        manager.putDomainSslContext("*.example.com", exampleCtx);
        assertTrue(manager.hasDomainConfigs());
    }

    @Test
    public void testGetDomainContexts() {
        manager.putDomainSslContext("*.example.com", exampleCtx);
        assertEquals(1, manager.getDomainContexts().size());
        assertSame(exampleCtx, manager.getDomainContexts().get("*.example.com"));
    }

    @Test
    public void testMultiLevelNoMatch() {
        manager.putDomainSslContext("*.example.com", exampleCtx);
        assertSame(defaultCtx, manager.resolve("a.b.example.com"));
    }

    @Test
    public void testNoDefaultContext() {
        TlsSniManager noDefault = new TlsSniManager();
        assertNull(noDefault.resolve("foo.example.com"));
    }
}
