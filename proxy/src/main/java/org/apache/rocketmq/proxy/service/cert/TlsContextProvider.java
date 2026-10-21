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

import io.netty.handler.ssl.SslContext;
import io.netty.util.Mapping;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

public class TlsContextProvider {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.PROXY_LOGGER_NAME);

    private volatile SslContext defaultSslContext;
    private final ConcurrentHashMap<String, SslContext> domainContexts = new ConcurrentHashMap<>();

    public void setDefaultSslContext(SslContext sslContext) {
        this.defaultSslContext = sslContext;
    }

    public SslContext getDefaultSslContext() {
        return this.defaultSslContext;
    }

    public void putDomainSslContext(String pattern, SslContext sslContext) {
        domainContexts.put(pattern.toLowerCase(Locale.ROOT), sslContext);
    }

    public void reloadDomain(String pattern, SslContext newContext) {
        String key = pattern.toLowerCase(Locale.ROOT);
        SslContext old = domainContexts.put(key, newContext);
        if (old != null) {
            releaseContext(old, pattern);
        }
    }

    public void reloadDefault(SslContext newContext) {
        SslContext old = this.defaultSslContext;
        this.defaultSslContext = newContext;
        if (old != null) {
            releaseContext(old, "default");
        }
    }

    public SslContext resolve(String hostname) {
        if (hostname == null || hostname.isEmpty()) {
            return defaultSslContext;
        }
        String matched = SniHostnameMatcher.findMatchingPattern(hostname, domainContexts.keySet());
        if (matched != null) {
            return domainContexts.get(matched);
        }
        if (defaultSslContext == null) {
            log.warn("No matching domain and no default SslContext for hostname: {}", hostname);
        }
        return defaultSslContext;
    }

    public Mapping<String, SslContext> asMapping() {
        return this::resolve;
    }

    public Map<String, SslContext> getDomainContexts() {
        return domainContexts;
    }

    public boolean hasDomainConfigs() {
        return !domainContexts.isEmpty();
    }

    private void releaseContext(SslContext ctx, String label) {
        try {
            io.netty.util.ReferenceCountUtil.release(ctx);
            log.info("Released old remoting SslContext for: {}", label);
        } catch (Exception e) {
            log.warn("Failed to release old remoting SslContext for: {}", label, e);
        }
    }
}
