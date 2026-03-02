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

package org.apache.rocketmq.common.utils;

import org.apache.rocketmq.common.Pair;
import org.apache.rocketmq.common.lite.LiteUtil;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class LiteUtilTest {

    @Test
    public void testToLmqName() {
        String result = LiteUtil.toLmqName("parentTopic", "liteTopic");
        String expected = LiteUtil.LITE_TOPIC_PREFIX + "parentTopic" + LiteUtil.SEPARATOR + "liteTopic";
        assertEquals(expected, result);

        assertNull(LiteUtil.toLmqName(null, "liteTopic"));
        assertNull(LiteUtil.toLmqName("parentTopic", null));
        assertNull(LiteUtil.toLmqName("", "liteTopic"));
        assertNull(LiteUtil.toLmqName("parentTopic", ""));
    }

    @Test
    public void testIsLiteTopicQueue() {
        assertTrue(LiteUtil.isLiteTopicQueue("%LMQ%$parentTopic$liteTopic"));

        assertFalse(LiteUtil.isLiteTopicQueue("%LMQ%parentTopic"));
        assertFalse(LiteUtil.isLiteTopicQueue("parentTopic"));
        assertFalse(LiteUtil.isLiteTopicQueue(null));
        assertFalse(LiteUtil.isLiteTopicQueue("%LMQ$"));
    }

    @Test
    public void testGetParentTopic() {
        assertEquals("parentTopic", LiteUtil.getParentTopic("%LMQ%$parentTopic$liteTopic"));

        assertNull(LiteUtil.getParentTopic(null));
        assertNull(LiteUtil.getParentTopic("parentTopic"));
        assertNull(LiteUtil.getParentTopic("%LMQ%parentTopic$liteTopic"));
        assertNull(LiteUtil.getParentTopic("%LMQ%$$"));
        assertNull(LiteUtil.getParentTopic("%LMQ%$parentTopic"));
        assertNull(LiteUtil.getParentTopic("%LMQ%$parentTopic$"));
        assertNull(LiteUtil.getParentTopic("%LMQ%$$liteTopic"));
        assertNull(LiteUtil.getParentTopic("%LMQ%$parent$lite$extra"));
    }

    @Test
    public void testGetLiteTopic() {
        assertEquals("liteTopic", LiteUtil.getLiteTopic("%LMQ%$parentTopic$liteTopic"));

        assertNull(LiteUtil.getLiteTopic(null));
        assertNull(LiteUtil.getLiteTopic("parentTopic"));
        assertNull(LiteUtil.getParentTopic("%LMQ%parentTopic$liteTopic"));
        assertNull(LiteUtil.getParentTopic("%LMQ%$$"));
        assertNull(LiteUtil.getLiteTopic("%LMQ%$parentTopic"));
        assertNull(LiteUtil.getLiteTopic("%LMQ%$parentTopic$"));
        assertNull(LiteUtil.getLiteTopic("%LMQ%$$liteTopic"));
        assertNull(LiteUtil.getLiteTopic("%LMQ%$parent$lite$extra"));
    }

    @Test
    public void testGetParentAndLiteTopic() {
        Pair<String, String> result = LiteUtil.getParentAndLiteTopic("%LMQ%$parentTopic$liteTopic");
        assertNotNull(result);
        assertEquals("parentTopic", result.getObject1());
        assertEquals("liteTopic", result.getObject2());

        assertNull(LiteUtil.getParentTopic(null));
        assertNull(LiteUtil.getParentTopic("parentTopic"));
        assertNull(LiteUtil.getParentTopic("%LMQ%parentTopic$liteTopic"));
        assertNull(LiteUtil.getParentTopic("%LMQ%$$"));
        assertNull(LiteUtil.getParentTopic("%LMQ%$parentTopic"));
        assertNull(LiteUtil.getParentTopic("%LMQ%$parentTopic$"));
        assertNull(LiteUtil.getParentTopic("%LMQ%$$liteTopic"));
        assertNull(LiteUtil.getParentTopic("%LMQ%$parent$lite$extra"));
    }

    @Test
    public void testBelongsTo() {
        assertTrue(LiteUtil.belongsTo("%LMQ%$parentTopic$liteTopic", "parentTopic"));
        assertTrue(LiteUtil.belongsTo("%LMQ%$parentTopic$", "parentTopic")); // only check prefix
        assertTrue(LiteUtil.belongsTo("%LMQ%$parentTopic$liteTopic$xxx", "parentTopic")); // only check prefix

        assertFalse(LiteUtil.belongsTo("%LMQ%$parentTopic$liteTopic", "otherParent"));
        assertFalse(LiteUtil.belongsTo("parentTopic", "parentTopic"));
        assertFalse(LiteUtil.belongsTo(null, "parentTopic"));
        assertFalse(LiteUtil.belongsTo("%LMQ%$parentTopic$liteTopic", null));
    }
}
