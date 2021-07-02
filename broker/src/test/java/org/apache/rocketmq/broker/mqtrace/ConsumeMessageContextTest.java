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
package org.apache.rocketmq.broker.mqtrace;

import org.apache.rocketmq.store.stats.BrokerStatsManager;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

public class ConsumeMessageContextTest {
    private Map<String, String> fields2Type = new HashMap<>();

    @Before
    public void init() {
        fields2Type.put("consumerGroup", String.class.toString());
        fields2Type.put("topic", String.class.toString());
        fields2Type.put("queueId", Integer.class.toString());
        fields2Type.put("clientHost", String.class.toString());
        fields2Type.put("storeHost", String.class.toString());
        fields2Type.put("messageIds", Map.class.toString());
        fields2Type.put("bodyLength", "int");
        fields2Type.put("success", "boolean");
        fields2Type.put("status", String.class.toString());
        fields2Type.put("mqTraceContext", Object.class.toString());
        fields2Type.put("commercialOwner", String.class.toString());
        fields2Type.put("commercialRcvStats", BrokerStatsManager.StatsType.class.toString());
        fields2Type.put("commercialRcvTimes", "int");
        fields2Type.put("commercialRcvSize", "int");
        fields2Type.put("namespace", String.class.toString());
    }

    @Test
    public void testContextFormat() {
        ConsumeMessageContext consumeMessageContext = new ConsumeMessageContext();
        Class cls = consumeMessageContext.getClass();
        Field[] fields = cls.getDeclaredFields();
        for(Field field : fields) {
            String name = field.getName();
            String clsName = field.getType().toString();
            String expectClsName = fields2Type.get(name);
            assertThat(clsName.equals(expectClsName)).isTrue();
        }
    }

}