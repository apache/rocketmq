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

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.netty.channel.ChannelHandlerContext;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.acl.common.SessionCredentials;
import org.apache.rocketmq.common.Pair;
import org.apache.rocketmq.common.constant.CommonConstants;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.ratelimit.config.RatelimitConfig;
import org.apache.rocketmq.ratelimit.context.RatelimitContext;
import org.apache.rocketmq.ratelimit.factory.RatelimitFactory;
import org.apache.rocketmq.ratelimit.manager.RatelimitMetadataManager;
import org.apache.rocketmq.ratelimit.model.RatelimitRule;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.RequestCode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultRatelimitProvider implements RatelimitProvider {

    private static final Logger log = LoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);
    protected RatelimitConfig ratelimitConfig;
    protected RatelimitMetadataManager ratelimitMetadataManager;
    private static final String TOPIC = "topic";
    private static final String B = "b";
    private static final String PRODUCE_FORMAT = "%s#P#%s";
    private static final String CONSUME_FORMAT = "%s#C#%s";
    private Cache<String, Ratelimiter> ratelimiterCache;

    @Override
    public void initialize(RatelimitConfig config) {
        this.ratelimitConfig = config;
        this.ratelimitMetadataManager = RatelimitFactory.getMetadataManager(ratelimitConfig);
        this.ratelimiterCache = Caffeine.newBuilder()
                .maximumSize(ratelimitConfig.getRatelimiterCacheMaxSize())
                .build();
    }


    @Override
    public RatelimitContext newContext(ChannelHandlerContext context, RemotingCommand command) {
        HashMap<String, String> fields = command.getExtFields();
        RatelimitContext result = new RatelimitContext();
        String remoteAddr = RemotingHelper.parseChannelRemoteAddr(context.channel());
        String sourceIp = StringUtils.substringBefore(remoteAddr, CommonConstants.COLON);
        result.setSourceIp(sourceIp);
        result.setChannelId(context.channel().id().asLongText());
        if (fields.containsKey(SessionCredentials.ACCESS_KEY)) {
            result.setAccessKey(fields.get(SessionCredentials.ACCESS_KEY));
        }
        String topic = null;
        switch (command.getCode()) {
            case RequestCode.SEND_MESSAGE:
            case RequestCode.PULL_MESSAGE:
            case RequestCode.LITE_PULL_MESSAGE:
            case RequestCode.POP_MESSAGE:
                topic = fields.get(TOPIC);
                break;
            case RequestCode.SEND_MESSAGE_V2:
            case RequestCode.SEND_BATCH_MESSAGE:
                topic = fields.get(B);
                break;
        }
        result.setTopic(topic);
        return result;
    }

    @Override
    public boolean produceTryAcquire(RatelimitContext context, int token) {
        Map<String, RatelimitRuleCache> ratelimitCache = ratelimitMetadataManager.getRatelimitCache();
        List<Pair<Ratelimiter, Ratelimiter.Config>> ratelimiters = new ArrayList<>(ratelimitCache.size());
        for (Map.Entry<String, RatelimitRuleCache> entry : ratelimitCache.entrySet()) {
            RatelimitRuleCache ruleCache = entry.getValue();
            RatelimitRule rule = ruleCache.getRule();
            double tps = rule.getProduceTps();
            if (tps <= 0) {
                continue;
            }
            String entityName = getEntityName(context, rule);
            if (!ruleCache.getMatcher().matches(entityName)) {
                continue;
            }
            String ratelimitKey = String.format(PRODUCE_FORMAT, rule.getName(), entityName);
            Ratelimiter rateLimiter = ratelimiterCache.get(ratelimitKey, k -> new Ratelimiter());
            if (rateLimiter != null) {
                ratelimiters.add(new Pair<>(rateLimiter, ruleCache.getProduceRatelimiterConfig()));
            }
        }
        long now = System.currentTimeMillis();
        for (Pair<Ratelimiter, Ratelimiter.Config> pair : ratelimiters) {
            if (!pair.getObject1().canAcquire(pair.getObject2(), now, token)) {
                return false;
            }
        }
        for (Pair<Ratelimiter, Ratelimiter.Config> pair : ratelimiters) {
            pair.getObject1().reserve(pair.getObject2(), now, token);
        }
        return true;
    }

    @Override
    public boolean consumeCanAcquire(RatelimitContext context, int token) {
        long now = System.currentTimeMillis();
        for (Map.Entry<String, RatelimitRuleCache> entry : ratelimitMetadataManager.getRatelimitCache().entrySet()) {
            RatelimitRuleCache ruleCache = entry.getValue();
            RatelimitRule rule = ruleCache.getRule();
            double tps = rule.getConsumeTps();
            if (tps <= 0) {
                continue;
            }
            String entityName = getEntityName(context, rule);
            if (!ruleCache.getMatcher().matches(entityName)) {
                continue;
            }
            String ratelimitKey = String.format(CONSUME_FORMAT, rule.getName(), entityName);
            Ratelimiter rateLimiter = ratelimiterCache.get(ratelimitKey, k -> new Ratelimiter());
            if (rateLimiter != null) {
                if (!rateLimiter.canAcquire(ruleCache.getConsumeRatelimiterConfig(), now, token)) {
                    return false;
                }
            }
        }
        return true;
    }

    @Override
    public void consumeReserve(RatelimitContext context, int token) {
        long now = System.currentTimeMillis();
        for (Map.Entry<String, RatelimitRuleCache> entry : ratelimitMetadataManager.getRatelimitCache().entrySet()) {
            RatelimitRuleCache ruleCache = entry.getValue();
            RatelimitRule rule = ruleCache.getRule();
            double tps = rule.getConsumeTps();
            if (tps <= 0) {
                continue;
            }
            String entityName = getEntityName(context, rule);
            if (!ruleCache.getMatcher().matches(entityName)) {
                continue;
            }
            String ratelimitKey = String.format(CONSUME_FORMAT, rule.getName(), entityName);
            Ratelimiter rateLimiter = ratelimiterCache.get(ratelimitKey, k -> new Ratelimiter());
            if (rateLimiter != null) {
                rateLimiter.reserve(ruleCache.getConsumeRatelimiterConfig(), now, token);
            }
        }
    }

    private static String getEntityName(RatelimitContext context, RatelimitRule rule) {
        switch (rule.getEntityType()) {
            case IP:
                return context.getSourceIp();
            case CHANNEL_ID:
                return context.getChannelId();
            case USER:
                return context.getAccessKey();
            case TOPIC:
                return context.getTopic();
        }
        return "";
    }
}
