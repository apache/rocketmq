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

import org.apache.commons.collections.CollectionUtils;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.utils.ExceptionUtils;
import org.apache.rocketmq.ratelimit.config.RatelimitConfig;
import org.apache.rocketmq.ratelimit.exception.RatelimitException;
import org.apache.rocketmq.ratelimit.factory.RatelimitFactory;
import org.apache.rocketmq.ratelimit.helper.RatelimitTestHelper;
import org.apache.rocketmq.ratelimit.model.EntityType;
import org.apache.rocketmq.ratelimit.model.MatcherType;
import org.apache.rocketmq.ratelimit.model.RatelimitRule;
import org.apache.rocketmq.ratelimit.provider.RatelimitRuleCache;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Collection;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class RatelimitMetadataManagerTest {
    private RatelimitConfig ratelimitConfig;
    private RatelimitMetadataManager ratelimitMetadataManager;

    @Before
    public void setUp() throws Exception {
        if (MixAll.isMac()) {
            return;
        }
        this.ratelimitConfig = RatelimitTestHelper.createDefaultConfig();
        this.ratelimitMetadataManager = RatelimitFactory.getMetadataManager(this.ratelimitConfig);
        this.clearAllRatelimitRules();
    }

    @After
    public void tearDown() throws Exception {
        if (MixAll.isMac()) {
            return;
        }
        this.clearAllRatelimitRules();
        this.ratelimitMetadataManager.shutdown();
    }

    @Test
    public void createRule() {
        if (MixAll.isMac()) {
            return;
        }
        RatelimitRule rule = RatelimitTestHelper.getRatelimitRule("r1");
        this.ratelimitMetadataManager.createRule(rule).join();
        RatelimitRule result = this.ratelimitMetadataManager.getRule("r1").join();
        Assert.assertNotNull(rule);
        assertTrue(rule.equals(result));

        Assert.assertThrows(RatelimitException.class, () -> {
            try {
                RatelimitRule r2 = new RatelimitRule();
                r2.setName("r1");
                r2.setEntityType(EntityType.TOPIC);
                r2.setEntityName("topic1");
                this.ratelimitMetadataManager.createRule(r2).join();
            } catch (Exception e) {
                throw ExceptionUtils.getRealException(e);
            }
        });
    }

    @Test
    public void createRuleMaxNum() {
        if (MixAll.isMac()) {
            return;
        }
        for (int i = 0; i < ratelimitConfig.getRatelimitRuleMaxNum(); i++) {
            RatelimitRule rule = RatelimitTestHelper.getRatelimitRule("r" + i);
            this.ratelimitMetadataManager.createRule(rule).join();
        }

        Assert.assertThrows(RatelimitException.class, () -> {
            try {
                RatelimitRule rule = RatelimitTestHelper.getRatelimitRule("more");
                this.ratelimitMetadataManager.createRule(rule).join();
            } catch (Exception e) {
                throw ExceptionUtils.getRealException(e);
            }
        });
    }


    @Test
    public void updateRule() {
        if (MixAll.isMac()) {
            return;
        }
        RatelimitRule rule = RatelimitTestHelper.getRatelimitRule("r1");
        this.ratelimitMetadataManager.createRule(rule).join();
        rule = this.ratelimitMetadataManager.getRule("r1").join();

        rule.setProduceTps(12.3);
        this.ratelimitMetadataManager.updateRule(rule).join();
        rule = this.ratelimitMetadataManager.getRule("r1").join();
        Assert.assertNotNull(rule);
        Assert.assertEquals(rule.getName(), "r1");
        Assert.assertEquals(rule.getProduceTps(), 12.3, 0.001);
        Assert.assertEquals(rule.getEntityType(), EntityType.TOPIC);

        rule.setEntityType(EntityType.IP);
        this.ratelimitMetadataManager.updateRule(rule).join();
        rule = this.ratelimitMetadataManager.getRule("r1").join();
        Assert.assertNotNull(rule);
        Assert.assertEquals(rule.getName(), "r1");
        Assert.assertEquals(rule.getProduceTps(), 12.3, 0.001);
        Assert.assertEquals(rule.getEntityType(), EntityType.IP);

        Assert.assertThrows(RatelimitException.class, () -> {
            try {
                RatelimitRule r2 = RatelimitTestHelper.getRatelimitRule("no_ratelimit");
                this.ratelimitMetadataManager.updateRule(r2).join();
            } catch (Exception e) {
                throw ExceptionUtils.getRealException(e);
            }
        });
    }

    @Test
    public void deleteRule() {
        if (MixAll.isMac()) {
            return;
        }
        RatelimitRule rule = RatelimitTestHelper.getRatelimitRule("test");
        this.ratelimitMetadataManager.createRule(rule).join();
        rule = this.ratelimitMetadataManager.getRule("test").join();
        Assert.assertNotNull(rule);
        this.ratelimitMetadataManager.deleteRule("test").join();
        rule = this.ratelimitMetadataManager.getRule("test").join();
        Assert.assertNull(rule);

        this.ratelimitMetadataManager.deleteRule("no_ratelimit").join();
    }

    @Test
    public void getRule() {
        RatelimitRule rule = this.ratelimitMetadataManager.getRule("no_user").join();
        Assert.assertNull(rule);
    }

    @Test
    public void listRule() {
        Collection<RatelimitRule> users = this.ratelimitMetadataManager.listRule().join();
        Assert.assertTrue(CollectionUtils.isEmpty(users));

        RatelimitRule rule = RatelimitTestHelper.getRatelimitRule("test");
        this.ratelimitMetadataManager.createRule(rule).join();
        users = this.ratelimitMetadataManager.listRule().join();
        Assert.assertEquals(users.size(), 1);

        rule = RatelimitTestHelper.getRatelimitRule("test2");
        this.ratelimitMetadataManager.createRule(rule).join();
        users = this.ratelimitMetadataManager.listRule().join();
        Assert.assertEquals(users.size(), 2);
    }

    @Test
    public void getRatelimitCache() {
        RatelimitRule rule = RatelimitTestHelper.getRatelimitRule("test");
        this.ratelimitMetadataManager.createRule(rule).join();
        Map<String, RatelimitRuleCache> cache = this.ratelimitMetadataManager.getRatelimitCache();
        assertEquals(1, cache.size());
        assertTrue(cache.get("test").getMatcher().matches("topic1"));
        assertNotNull(cache.get("test").getConsumeRatelimiterConfig());
        assertNotNull(cache.get("test").getProduceRatelimiterConfig());

        rule.setMatcherType(MatcherType.STARTSWITH);
        this.ratelimitMetadataManager.updateRule(rule).join();
        cache = this.ratelimitMetadataManager.getRatelimitCache();
        assertEquals(1, cache.size());
        assertTrue(cache.get("test").getMatcher().matches("topic1123"));

        this.ratelimitMetadataManager.deleteRule(rule.getName()).join();
        cache = this.ratelimitMetadataManager.getRatelimitCache();
        assertEquals(0, cache.size());
    }

    private void clearAllRatelimitRules() {
        Collection<RatelimitRule> rules = this.ratelimitMetadataManager.listRule().join();
        if (CollectionUtils.isEmpty(rules)) {
            return;
        }
        rules.forEach(rule -> this.ratelimitMetadataManager.deleteRule(rule.getName()).join());
    }
}