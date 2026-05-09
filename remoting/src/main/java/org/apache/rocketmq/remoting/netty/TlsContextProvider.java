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
package org.apache.rocketmq.remoting.netty;

import io.netty.handler.ssl.SslContext;

/**
 * Holder for SslContext used by remoting server's TlsModeHandler.
 * Proxy module initializes this with either a single SslContext or a TlsSniManager-backed provider.
 */
public class TlsContextProvider {

    private static volatile TlsContextProvider instance = new TlsContextProvider();

    private volatile SslContext singleContext;
    private volatile SniContextLookup sniLookup;

    public static TlsContextProvider getInstance() {
        return instance;
    }

    public static void setInstance(TlsContextProvider provider) {
        instance = provider;
    }

    /**
     * Set a single SslContext (backward compatible mode).
     */
    public void setSingleContext(SslContext ctx) {
        this.singleContext = ctx;
        this.sniLookup = null;
    }

    /**
     * Set an SNI-aware context lookup.
     */
    public void setSniLookup(SniContextLookup lookup) {
        this.sniLookup = lookup;
        this.singleContext = null;
    }

    /**
     * Get the SslContext for a given SNI hostname. Returns singleContext when no SNI lookup is configured.
     */
    public SslContext getSslContext(String sniHostname) {
        if (sniLookup != null) {
            SslContext ctx = sniLookup.lookup(sniHostname);
            if (ctx != null) {
                return ctx;
            }
        }
        return singleContext;
    }

    /**
     * Get the default SslContext for fallback.
     */
    public SslContext getDefaultContext() {
        if (sniLookup != null) {
            return sniLookup.getDefaultContext();
        }
        return singleContext;
    }

    /**
     * Returns the SniContextLookup if configured, null otherwise.
     */
    public SniContextLookup getSniLookup() {
        return sniLookup;
    }

    /**
     * Interface for SNI-aware context lookup, implemented in proxy module.
     */
    public interface SniContextLookup {
        SslContext lookup(String sniHostname);
        SslContext getDefaultContext();
    }
}
