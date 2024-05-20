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
package org.apache.rocketmq.ratelimit.manager;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.ratelimit.exception.RatelimitException;
import org.apache.rocketmq.ratelimit.factory.RatelimitFactory;
import org.apache.rocketmq.ratelimit.model.MatcherType;
import org.apache.rocketmq.ratelimit.model.RatelimitRule;
import org.apache.rocketmq.ratelimit.provider.RatelimitMetadataProvider;
import org.apache.rocketmq.ratelimit.config.RatelimitConfig;
import org.apache.rocketmq.common.utils.ExceptionUtils;
import org.apache.rocketmq.ratelimit.provider.RatelimitRuleCache;

public class RatelimitMetadataManagerImpl implements RatelimitMetadataManager {

    private final RatelimitMetadataProvider ratelimitMetadataProvider;
    private Map<String, RatelimitRuleCache> ratelimitCache = new ConcurrentHashMap<>();

    public RatelimitMetadataManagerImpl(RatelimitConfig ratelimitConfig) {
        this.ratelimitMetadataProvider = RatelimitFactory.getMetadataProvider(ratelimitConfig);
        this.init(ratelimitConfig);
    }

    @Override
    public void init(RatelimitConfig ratelimitConfig) {
        listRule()
                .thenAccept(rules -> {
                    for (RatelimitRule rule : rules) {
                        ratelimitCache.put(rule.getName(), new RatelimitRuleCache(rule));
                    }
                })
                .join();
    }

    @Override
    public void shutdown() {
        if (this.ratelimitMetadataProvider != null) {
            this.ratelimitMetadataProvider.shutdown();
        }
    }

    @Override
    public CompletableFuture<Void> createRule(RatelimitRule rule) {
        CompletableFuture<Void> result = new CompletableFuture<>();
        try {
            if (rule.getMatcherType() == null) {
                rule.setMatcherType(MatcherType.IN);
            }
            this.validate(rule, true);
            result = this.getRatelimitMetadataProvider().getRule(rule.getName()).thenCompose(old -> {
                if (old != null) {
                    throw new RatelimitException("The rule is existed");
                }
                return this.getRatelimitMetadataProvider().createRule(rule);
            }).thenAccept(suc -> {
                ratelimitCache.put(rule.getName(), new RatelimitRuleCache(rule));
            });
        } catch (Exception e) {
            this.handleException(e, result);
        }
        return result;
    }

    @Override
    public CompletableFuture<Void> updateRule(RatelimitRule rule) {
        CompletableFuture<Void> result = new CompletableFuture<>();
        try {
            this.validate(rule, false);
            AtomicReference<RatelimitRule> updated = new AtomicReference<>();
            result = this.getRatelimitMetadataProvider().getRule(rule.getName()).thenCompose(old -> {
                if (old == null) {
                    throw new RatelimitException("The rule is not exist");
                }
                RatelimitRule newRule = new RatelimitRule(old);
                if (rule.getEntityType() != null) {
                    newRule.setEntityType(rule.getEntityType());
                }
                if (StringUtils.isNotBlank(rule.getEntityName())) {
                    newRule.setEntityName(rule.getEntityName());
                }
                if (rule.getMatcherType() != null) {
                    newRule.setMatcherType(rule.getMatcherType());
                }
                newRule.setProduceTps(rule.getProduceTps());
                newRule.setConsumeTps(rule.getConsumeTps());
                updated.set(newRule);
                return this.getRatelimitMetadataProvider().updateRule(newRule);
            }).thenAccept(suc -> {
                ratelimitCache.put(rule.getName(), new RatelimitRuleCache(updated.get()));
            });
        } catch (Exception e) {
            this.handleException(e, result);
        }
        return result;
    }

    @Override
    public CompletableFuture<Void> deleteRule(String ratelimitName) {
        CompletableFuture<Void> result = new CompletableFuture<>();
        try {
            if (StringUtils.isBlank(ratelimitName)) {
                throw new RatelimitException("ratelimitName can not be blank");
            }
            return this.getRatelimitMetadataProvider().deleteRule(ratelimitName).thenAccept(suc -> {
                ratelimitCache.remove(ratelimitName);
            });
        } catch (Exception e) {
            this.handleException(e, result);
        }
        return result;
    }

    @Override
    public CompletableFuture<RatelimitRule> getRule(String ratelimitName) {
        CompletableFuture<RatelimitRule> result = new CompletableFuture<>();
        try {
            if (StringUtils.isBlank(ratelimitName)) {
                throw new RatelimitException("ratelimitName can not be blank");
            }
            result = this.getRatelimitMetadataProvider().getRule(ratelimitName);
        } catch (Exception e) {
            this.handleException(e, result);
        }
        return result;
    }

    @Override
    public CompletableFuture<Collection<RatelimitRule>> listRule() {
        CompletableFuture<Collection<RatelimitRule>> result = new CompletableFuture<>();
        try {
            result = this.getRatelimitMetadataProvider().listRule();
        } catch (Exception e) {
            this.handleException(e, result);
        }
        return result;
    }

    private void validate(RatelimitRule rule, boolean isCreate) {
        if (rule == null) {
            throw new RatelimitException("rule can not be null");
        }
        if (StringUtils.isBlank(rule.getName())) {
            throw new RatelimitException("rule name can not be blank");
        }
        if (isCreate && rule.getEntityType() == null) {
            throw new RatelimitException("rule entity type can not be null");
        }
        if (isCreate && StringUtils.isBlank(rule.getEntityName())) {
            throw new RatelimitException("rule entity name can not be blank");
        }
        if (isCreate && rule.getMatcherType() == null) {
            throw new RatelimitException("rule matcher type can not be null");
        }
        if (rule.getProduceTps() < -1) {
            throw new RatelimitException("rule produceTps can not < -1");
        }
        if (rule.getConsumeTps() < -1) {
            throw new RatelimitException("rule consumeTps can not < -1");
        }
    }

    private void handleException(Exception e, CompletableFuture<?> result) {
        Throwable throwable = ExceptionUtils.getRealException(e);
        result.completeExceptionally(throwable);
    }

    private RatelimitMetadataProvider getRatelimitMetadataProvider() {
        if (ratelimitMetadataProvider == null) {
            throw new IllegalStateException("The ratelimitMetadataProvider is not configured");
        }
        return ratelimitMetadataProvider;
    }

    @Override
    public Map<String, RatelimitRuleCache> getRatelimitCache() {
        return ratelimitCache;
    }
}
