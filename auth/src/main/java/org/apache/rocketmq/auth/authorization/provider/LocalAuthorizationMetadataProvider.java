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
package org.apache.rocketmq.auth.authorization.provider;

import com.alibaba.fastjson2.JSON;
import com.github.benmanes.caffeine.cache.CacheLoader;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.auth.authentication.model.Subject;
import org.apache.rocketmq.auth.authorization.exception.AuthorizationException;
import org.apache.rocketmq.auth.authorization.model.Acl;
import org.apache.rocketmq.auth.authorization.model.Policy;
import org.apache.rocketmq.auth.authorization.model.PolicyEntry;
import org.apache.rocketmq.auth.config.AuthConfig;
import org.apache.rocketmq.common.config.ConfigRocksDBStorage;
import org.apache.rocketmq.common.thread.ThreadPoolMonitor;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.rocksdb.RocksIterator;

public class LocalAuthorizationMetadataProvider implements AuthorizationMetadataProvider {

    private ConfigRocksDBStorage storage;

    private LoadingCache<String, Acl> aclCache;

    @Override
    public void initialize(AuthConfig authConfig, Supplier<?> metadataService) {
        this.storage = new ConfigRocksDBStorage(authConfig.getAuthConfigPath() + File.separator + "acls");
        if (!this.storage.start()) {
            throw new RuntimeException("Failed to load rocksdb for auth_acl, please check whether it is occupied.");
        }
        ThreadPoolExecutor cacheRefreshExecutor = ThreadPoolMonitor.createAndMonitor(
            1,
            1,
            1000 * 60,
            TimeUnit.MILLISECONDS,
            "AclCacheRefresh",
            100000
        );

        this.aclCache = Caffeine.newBuilder()
            .maximumSize(authConfig.getAclCacheMaxNum())
            .expireAfterAccess(authConfig.getAclCacheExpiredSecond(), TimeUnit.SECONDS)
            .refreshAfterWrite(authConfig.getAclCacheRefreshSecond(), TimeUnit.SECONDS)
            .executor(cacheRefreshExecutor)
            .build(new AclCacheLoader(this.storage));
    }

    @Override
    public CompletableFuture<Void> createAcl(Acl acl) {
        try {
            Subject subject = acl.getSubject();
            byte[] keyBytes = subject.getSubjectKey().getBytes(StandardCharsets.UTF_8);
            byte[] valueBytes = JSON.toJSONBytes(acl);
            this.storage.put(keyBytes, keyBytes.length, valueBytes);
            this.storage.flushWAL();
            this.aclCache.invalidate(subject.getSubjectKey());
        } catch (Exception e) {
            throw new AuthorizationException("create Acl to RocksDB failed.", e);
        }
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Void> deleteAcl(Subject subject) {
        try {
            byte[] keyBytes = subject.getSubjectKey().getBytes(StandardCharsets.UTF_8);
            this.storage.delete(keyBytes);
            this.storage.flushWAL();
            this.aclCache.invalidate(subject.getSubjectKey());
        } catch (Exception e) {
            throw new AuthorizationException("delete Acl from RocksDB failed.", e);
        }
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Void> updateAcl(Acl acl) {
        try {
            Subject subject = acl.getSubject();
            byte[] keyBytes = subject.getSubjectKey().getBytes(StandardCharsets.UTF_8);
            byte[] valueBytes = JSON.toJSONBytes(acl);
            this.storage.put(keyBytes, keyBytes.length, valueBytes);
            this.storage.flushWAL();
            this.aclCache.invalidate(subject.getSubjectKey());
        } catch (Exception e) {
            throw new AuthorizationException("update Acl to RocksDB failed.", e);
        }
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Acl> getAcl(Subject subject) {
        Acl acl = aclCache.get(subject.getSubjectKey());
        if (acl == AclCacheLoader.EMPTY_ACL) {
            return CompletableFuture.completedFuture(null);
        }
        return CompletableFuture.completedFuture(acl);
    }

    @Override
    public CompletableFuture<List<Acl>> listAcl(String subjectFilter, String resourceFilter) {
        List<Acl> result = new ArrayList<>();
        try (RocksIterator iterator = this.storage.iterator()) {
            iterator.seekToFirst();
            while (iterator.isValid()) {
                String subjectKey = new String(iterator.key(), StandardCharsets.UTF_8);
                if (StringUtils.isNotBlank(subjectFilter) && !subjectKey.contains(subjectFilter)) {
                    iterator.next();
                    continue;
                }
                Subject subject = Subject.of(subjectKey);
                Acl acl = JSON.parseObject(new String(iterator.value(), StandardCharsets.UTF_8), Acl.class);
                List<Policy> policies = acl.getPolicies();
                if (!CollectionUtils.isNotEmpty(policies)) {
                    iterator.next();
                    continue;
                }
                Iterator<Policy> policyIterator = policies.iterator();
                while (policyIterator.hasNext()) {
                    Policy policy = policyIterator.next();
                    List<PolicyEntry> entries = policy.getEntries();
                    if (CollectionUtils.isEmpty(entries)) {
                        continue;
                    }
                    if (StringUtils.isNotBlank(resourceFilter) && !subjectKey.contains(resourceFilter)) {
                        entries.removeIf(entry -> !entry.toResourceStr().contains(resourceFilter));
                    }
                    if (CollectionUtils.isEmpty(entries)) {
                        policyIterator.remove();
                    }
                }
                if (CollectionUtils.isNotEmpty(policies)) {
                    result.add(Acl.of(subject, policies));
                }
                iterator.next();
            }
        }
        return CompletableFuture.completedFuture(result);
    }

    @Override
    public void shutdown() {
        if (this.storage != null) {
            this.storage.shutdown();
        }
    }

    private static class AclCacheLoader implements CacheLoader<String, Acl> {
        private final ConfigRocksDBStorage storage;
        public static final Acl EMPTY_ACL = new Acl();

        public AclCacheLoader(ConfigRocksDBStorage storage) {
            this.storage = storage;
        }

        @Override
        public Acl load(@NonNull String subjectKey) {
            try {
                byte[] keyBytes = subjectKey.getBytes(StandardCharsets.UTF_8);
                Subject subject = Subject.of(subjectKey);

                byte[] valueBytes = this.storage.get(keyBytes);
                if (ArrayUtils.isEmpty(valueBytes)) {
                    return EMPTY_ACL;
                }
                Acl acl = JSON.parseObject(valueBytes, Acl.class);
                return Acl.of(subject, acl.getPolicies());
            } catch (Exception e) {
                throw new AuthorizationException("get Acl from RocksDB failed.", e);
            }
        }
    }
}
