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

import org.apache.rocketmq.ratelimit.model.EntityType;
import org.apache.rocketmq.ratelimit.model.MatcherType;
import org.apache.rocketmq.ratelimit.model.RatelimitRule;
import org.apache.rocketmq.remoting.protocol.body.RatelimitInfo;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class RatelimitConverterTest {


    @Test
    void testConvertRatelimit() {
        RatelimitInfo ratelimitInfo = new RatelimitInfo("ratelimit1", EntityType.TOPIC.name(), "topic1", MatcherType.IN.getName(), 10, 20);
        RatelimitRule ratelimitRule = RatelimitConverter.convertRatelimit(ratelimitInfo);
        assertNotNull(ratelimitRule);
        assertEquals("ratelimit1", ratelimitRule.getName());
        assertEquals(EntityType.TOPIC, ratelimitRule.getEntityType());
        assertEquals("topic1", ratelimitRule.getEntityName());
        assertEquals(MatcherType.IN, ratelimitRule.getMatcherType());
        assertEquals(10, ratelimitRule.getProduceTps(), 0.01);
        assertEquals(20, ratelimitRule.getConsumeTps(), 0.01);
    }

    @Test
    void testConvertRatelimit2() {
        RatelimitRule rule = new RatelimitRule();
        rule.setName("ratelimit1");
        rule.setEntityType(EntityType.TOPIC);
        rule.setEntityName("topic1");
        rule.setMatcherType(MatcherType.IN);
        rule.setProduceTps(10);
        rule.setConsumeTps(20);
        List<RatelimitInfo> ratelimitInfos = RatelimitConverter.convertRatelimit(Collections.singletonList(rule));
        assertNotNull(ratelimitInfos);
        assertEquals(1, ratelimitInfos.size());
        RatelimitInfo ratelimitInfo = ratelimitInfos.get(0);
        assertEquals("ratelimit1", ratelimitInfo.getName());
        assertEquals(EntityType.TOPIC.getName(), ratelimitInfo.getEntityType());
        assertEquals("topic1", ratelimitInfo.getEntityName());
        assertEquals(MatcherType.IN.getName(), ratelimitInfo.getMatcherType());
        assertEquals(10, ratelimitInfo.getProduceTps(), 0.01);
        assertEquals(20, ratelimitInfo.getConsumeTps(), 0.01);
    }
}