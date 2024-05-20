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
package org.apache.rocketmq.ratelimit.provider;

import com.alibaba.fastjson2.JSON;

import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Supplier;

import org.apache.rocketmq.ratelimit.exception.RatelimitException;
import org.apache.rocketmq.ratelimit.model.RatelimitRule;
import org.apache.rocketmq.ratelimit.config.RatelimitConfig;
import org.apache.rocketmq.common.config.ConfigRocksDBStorage;
import org.rocksdb.RocksIterator;

public class LocalRatelimitMetadataProvider implements RatelimitMetadataProvider {

    private ConfigRocksDBStorage storage;

    private ConcurrentMap<String, RatelimitRule> ratelimitCache;

    private RatelimitConfig ratelimitConfig;

    @Override
    public void initialize(RatelimitConfig ratelimitConfig, Supplier<?> metadataService) {
        this.ratelimitConfig = ratelimitConfig;
        this.storage = new ConfigRocksDBStorage(ratelimitConfig.getRatelimitConfigPath());
        if (!this.storage.start()) {
            throw new RuntimeException("Failed to load rocksdb for ratelimit, please check whether it is occupied");
        }
        this.ratelimitCache = new ConcurrentHashMap<>();
        try (RocksIterator iterator = this.storage.iterator()) {
            iterator.seekToFirst();
            while (iterator.isValid()) {
                String name = new String(iterator.key(), StandardCharsets.UTF_8);
                RatelimitRule user = JSON.parseObject(new String(iterator.value(), StandardCharsets.UTF_8), RatelimitRule.class);
                ratelimitCache.put(name, user);
                iterator.next();
            }
        }
    }

    @Override
    public CompletableFuture<Void> createRule(RatelimitRule rule) {
        if (ratelimitCache.size() >= ratelimitConfig.getRatelimitRuleMaxNum()) {
            throw new RatelimitException("ratelimit rule exceeds max num");
        }
        try {
            byte[] keyBytes = rule.getName().getBytes(StandardCharsets.UTF_8);
            byte[] valueBytes = JSON.toJSONBytes(rule);
            this.storage.put(keyBytes, keyBytes.length, valueBytes);
            this.storage.flushWAL();
            this.ratelimitCache.put(rule.getName(), rule);
        } catch (Exception e) {
            throw new RatelimitException("create ratelimit rule to RocksDB failed", e);
        }
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Void> deleteRule(String ratelimitName) {
        try {
            this.storage.delete(ratelimitName.getBytes(StandardCharsets.UTF_8));
            this.storage.flushWAL();
            this.ratelimitCache.remove(ratelimitName);
        } catch (Exception e) {
            throw new RatelimitException("delete ratelimit rule from RocksDB failed", e);
        }
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Void> updateRule(RatelimitRule rule) {
        try {
            byte[] keyBytes = rule.getName().getBytes(StandardCharsets.UTF_8);
            byte[] valueBytes = JSON.toJSONBytes(rule);
            this.storage.put(keyBytes, keyBytes.length, valueBytes);
            this.storage.flushWAL();
            this.ratelimitCache.put(rule.getName(), rule);
        } catch (Exception e) {
            throw new RatelimitException("update ratelimit rule to RocksDB failed", e);
        }
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<RatelimitRule> getRule(String name) {
        return CompletableFuture.completedFuture(this.ratelimitCache.get(name));
    }

    @Override
    public CompletableFuture<Collection<RatelimitRule>> listRule() {
        return CompletableFuture.completedFuture(this.ratelimitCache.values());
    }

    @Override
    public void shutdown() {
        if (this.storage != null) {
            this.storage.shutdown();
        }
    }

}
