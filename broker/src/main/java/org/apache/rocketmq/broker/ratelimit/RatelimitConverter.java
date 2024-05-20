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
package org.apache.rocketmq.broker.ratelimit;

import org.apache.rocketmq.ratelimit.exception.RatelimitException;
import org.apache.rocketmq.ratelimit.model.EntityType;
import org.apache.rocketmq.ratelimit.model.MatcherType;
import org.apache.rocketmq.ratelimit.model.RatelimitRule;
import org.apache.rocketmq.remoting.protocol.body.RatelimitInfo;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class RatelimitConverter {

    public static RatelimitRule convertRatelimit(RatelimitInfo ratelimitInfo) {
        if (ratelimitInfo == null) {
            return null;
        }
        RatelimitRule rule = new RatelimitRule();
        rule.setName(ratelimitInfo.getName());
        if (ratelimitInfo.getEntityType() != null) {
            EntityType entityType = EntityType.getByName(ratelimitInfo.getEntityType());
            if (entityType == null) {
                throw new RatelimitException(String.format("invalid entityType %s", ratelimitInfo.getEntityType()));
            }
            rule.setEntityType(entityType);
        }
        rule.setEntityName(ratelimitInfo.getEntityName());
        if (ratelimitInfo.getMatcherType() != null) {
            MatcherType matcherType = MatcherType.getByName(ratelimitInfo.getMatcherType());
            if (matcherType == null) {
                throw new RatelimitException(String.format("invalid matcherType %s", ratelimitInfo.getMatcherType()));
            }
            rule.setMatcherType(matcherType);
        }
        rule.setProduceTps(ratelimitInfo.getProduceTps());
        rule.setConsumeTps(ratelimitInfo.getConsumeTps());
        return rule;
    }

    public static List<RatelimitInfo> convertRatelimit(Collection<RatelimitRule> ratelimitRules) {
        if (ratelimitRules == null) {
            return null;
        }
        List<RatelimitInfo> ratelimitInfos = new ArrayList<>();
        for (RatelimitRule rule : ratelimitRules) {
            ratelimitInfos.add(new RatelimitInfo(
                    rule.getName(),
                    rule.getEntityType().getName(),
                    rule.getEntityName(),
                    rule.getMatcherType().getName(),
                    rule.getProduceTps(),
                    rule.getConsumeTps()
            ));
        }
        return ratelimitInfos;
    }
}
