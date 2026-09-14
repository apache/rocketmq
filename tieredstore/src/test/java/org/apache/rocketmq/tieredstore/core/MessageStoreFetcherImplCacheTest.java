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
package org.apache.rocketmq.tieredstore.core;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Ticker;
import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.tieredstore.MessageStoreConfig;
import org.apache.rocketmq.tieredstore.common.SelectBufferResult;
import org.junit.Assert;
import org.junit.Test;

public class MessageStoreFetcherImplCacheTest {

    private static SelectBufferResult buffer(int size) {
        return new SelectBufferResult(ByteBuffer.allocate(size), 0L, size, 0L);
    }

    @Test
    public void resolveReadAheadCacheTtl_defaultConfigUsesDualTtl() {
        MessageStoreConfig config = new MessageStoreConfig();

        MessageStoreFetcherImpl.ReadAheadCacheTtl ttl = MessageStoreFetcherImpl.resolveReadAheadCacheTtl(config);

        Assert.assertNotNull(ttl);
        Assert.assertEquals(180_000L, ttl.createMs);
        Assert.assertEquals(10_000L, ttl.afterReadMs);
    }

    @Test
    public void resolveReadAheadCacheTtl_bothDualTtlsDisabledUsesLegacyFallback() {
        MessageStoreConfig config = new MessageStoreConfig();
        config.setReadAheadCacheCreateExpireDuration(0);
        config.setReadAheadCacheAfterReadExpireDuration(0);

        Assert.assertNull(MessageStoreFetcherImpl.resolveReadAheadCacheTtl(config));
    }

    @Test
    public void resolveReadAheadCacheTtl_oneDualTtlDisabledUsesLegacyDuration() {
        MessageStoreConfig config = new MessageStoreConfig();
        config.setReadAheadCacheExpireDuration(15_000L);
        config.setReadAheadCacheCreateExpireDuration(0);
        config.setReadAheadCacheAfterReadExpireDuration(10_000L);

        MessageStoreFetcherImpl.ReadAheadCacheTtl ttl = MessageStoreFetcherImpl.resolveReadAheadCacheTtl(config);

        Assert.assertNotNull(ttl);
        Assert.assertEquals(15_000L, ttl.createMs);
        Assert.assertEquals(10_000L, ttl.afterReadMs);
    }

    @Test
    public void resolveReadAheadCacheTtl_invalidPartialFallbackUsesLegacyFallback() {
        MessageStoreConfig config = new MessageStoreConfig();
        config.setReadAheadCacheExpireDuration(0);
        config.setReadAheadCacheCreateExpireDuration(180_000L);
        config.setReadAheadCacheAfterReadExpireDuration(0);

        Assert.assertNull(MessageStoreFetcherImpl.resolveReadAheadCacheTtl(config));
    }

    @Test
    public void dualTtlCache_unreadEntryExpiresOnCreateTtl() {
        FakeTicker ticker = new FakeTicker();
        Cache<String, SelectBufferResult> cache =
            MessageStoreFetcherImpl.buildDualTtlCache(180_000L, 10_000L, 1L << 30, ticker);

        cache.put("key", buffer(100));
        ticker.advanceMs(180_001L);
        cache.cleanUp();
        Assert.assertNull(cache.getIfPresent("key"));
    }

    @Test
    public void dualTtlExpiryUsesCreateTtlUntilRead() {
        SelectBufferResult buffer = buffer(100);
        MessageStoreFetcherImpl.DualTtlExpiry expiry =
            new MessageStoreFetcherImpl.DualTtlExpiry(180_000L, 10_000L);

        Assert.assertEquals(
            TimeUnit.MILLISECONDS.toNanos(180_000L), expiry.expireAfterCreate("key", buffer, 0L));
        Assert.assertEquals(
            TimeUnit.MILLISECONDS.toNanos(180_000L), expiry.expireAfterUpdate("key", buffer, 0L, 1L));
        Assert.assertEquals(
            TimeUnit.MILLISECONDS.toNanos(10_000L), expiry.expireAfterRead("key", buffer, 0L, 1L));
    }

    @Test
    public void dualTtlCache_readEntryExpiresOnAfterReadTtl() {
        FakeTicker ticker = new FakeTicker();
        Cache<String, SelectBufferResult> cache =
            MessageStoreFetcherImpl.buildDualTtlCache(180_000L, 10_000L, 1L << 30, ticker);

        cache.put("key", buffer(100));
        ticker.advanceMs(1_000L);
        Assert.assertNotNull(cache.getIfPresent("key"));

        ticker.advanceMs(15_000L);
        cache.cleanUp();
        Assert.assertNull(cache.getIfPresent("key"));
    }

    @Test
    public void dualTtlCache_readThenRePutResetsToCreateTtl() {
        FakeTicker ticker = new FakeTicker();
        Cache<String, SelectBufferResult> cache =
            MessageStoreFetcherImpl.buildDualTtlCache(180_000L, 10_000L, 1L << 30, ticker);

        cache.put("key", buffer(100));
        ticker.advanceMs(1_000L);
        Assert.assertNotNull(cache.getIfPresent("key"));
        cache.put("key", buffer(100));

        ticker.advanceMs(60_000L);
        cache.cleanUp();
        Assert.assertNotNull(cache.getIfPresent("key"));
    }

    @Test
    public void readAheadCacheTtl_afterReadNotShorterIsValidAndNotRewritten() {
        MessageStoreConfig config = new MessageStoreConfig();
        config.setReadAheadCacheCreateExpireDuration(100L);
        config.setReadAheadCacheAfterReadExpireDuration(200L);

        MessageStoreFetcherImpl.ReadAheadCacheTtl ttl = MessageStoreFetcherImpl.resolveReadAheadCacheTtl(config);

        Assert.assertNotNull(ttl);
        Assert.assertEquals(100L, ttl.createMs);
        Assert.assertEquals(200L, ttl.afterReadMs);
        Assert.assertTrue(ttl.isAfterReadNotShorter());
    }

    private static class FakeTicker implements Ticker {
        private long nanos;

        @Override
        public long read() {
            return nanos;
        }

        private void advanceMs(long ms) {
            nanos += TimeUnit.MILLISECONDS.toNanos(ms);
        }
    }
}
