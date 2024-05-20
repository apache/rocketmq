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

import org.apache.rocketmq.ratelimit.model.AnyMatcher;
import org.apache.rocketmq.ratelimit.model.CidrMatcher;
import org.apache.rocketmq.ratelimit.model.EndswithMatcher;
import org.apache.rocketmq.ratelimit.model.InMatcher;
import org.apache.rocketmq.ratelimit.model.Matcher;
import org.apache.rocketmq.ratelimit.model.RatelimitRule;
import org.apache.rocketmq.ratelimit.model.StartswithMatcher;

public class RatelimitRuleCache {

    private RatelimitRule rule;

    private Matcher matcher;

    private Ratelimiter.Config produceRatelimiterConfig;
    private Ratelimiter.Config consumeRatelimiterConfig;

    public RatelimitRuleCache(RatelimitRule rule) {
        setRule(rule);
        if (rule.getProduceTps() > 0) {
            produceRatelimiterConfig = new Ratelimiter.Config(rule.getProduceTps(), rule.getProduceTps());
        }
        if (rule.getConsumeTps() > 0) {
            consumeRatelimiterConfig = new Ratelimiter.Config(rule.getConsumeTps(), rule.getConsumeTps());
        }
    }

    public RatelimitRule getRule() {
        return rule;
    }

    public void setRule(RatelimitRule rule) {
        this.rule = rule;
        switch (rule.getMatcherType()) {
            case ANY:
                matcher = new AnyMatcher();
                break;
            case STARTSWITH:
                matcher = new StartswithMatcher(rule.getEntityName());
                break;
            case ENDSWITH:
                matcher = new EndswithMatcher(rule.getEntityName());
                break;
            case IN:
                matcher = new InMatcher(rule.getEntityName());
                break;
            case CIDR:
                matcher = new CidrMatcher(rule.getEntityName());
                break;
        }
    }

    public Matcher getMatcher() {
        return matcher;
    }

    public Ratelimiter.Config getProduceRatelimiterConfig() {
        return produceRatelimiterConfig;
    }

    public Ratelimiter.Config getConsumeRatelimiterConfig() {
        return consumeRatelimiterConfig;
    }
}
