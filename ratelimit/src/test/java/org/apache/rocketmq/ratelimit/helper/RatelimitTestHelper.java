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
package org.apache.rocketmq.ratelimit.helper;

import org.apache.rocketmq.ratelimit.config.RatelimitConfig;
import org.apache.rocketmq.ratelimit.model.EntityType;
import org.apache.rocketmq.ratelimit.model.RatelimitRule;
import org.jetbrains.annotations.NotNull;

public class RatelimitTestHelper {

    public static RatelimitConfig createDefaultConfig() {
        RatelimitConfig authConfig = new RatelimitConfig();
        authConfig.setRatelimitConfigPath("~/config");
        authConfig.setRatelimitEnabled(true);
        return authConfig;
    }

    public static @NotNull RatelimitRule getRatelimitRule(String name) {
        RatelimitRule rule = new RatelimitRule();
        rule.setName(name);
        rule.setEntityType(EntityType.TOPIC);
        rule.setEntityName("topic1");
        rule.setProduceTps(10);
        rule.setConsumeTps(20);
        return rule;
    }


}
