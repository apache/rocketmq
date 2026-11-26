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
package org.apache.rocketmq.proxy.service.sysmessage;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class HeartbeatSyncerDataTest {
    @Test
    public void testExtInfoSurvivesRoundTrip() {
        JSONObject original = new JSONObject();
        original.put("group", "consumer-group");
        original.put("channelData", "opaque-channel-data");
        JSONObject extInfo = new JSONObject();
        extInfo.put("tenant", "tenant-a");
        extInfo.put("future-key", "future-value");
        original.put("extInfo", extInfo);

        HeartbeatSyncerData decoded = original.toJavaObject(HeartbeatSyncerData.class);
        JSONObject encoded = JSON.parseObject(JSON.toJSONBytes(decoded));

        assertEquals(extInfo, encoded.getJSONObject("extInfo"));
        assertEquals(original.getString("group"), encoded.getString("group"));
        assertEquals(original.getString("channelData"), encoded.getString("channelData"));
    }

    @Test
    public void testLegacyMessageWithoutExtInfo() {
        HeartbeatSyncerData decoded = JSON.parseObject("{\"group\":\"consumer-group\"}", HeartbeatSyncerData.class);
        JSONObject encoded = JSON.parseObject(JSON.toJSONBytes(decoded));

        assertEquals("consumer-group", decoded.getGroup());
        assertNull(encoded.get("extInfo"));
    }
}
