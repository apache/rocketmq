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
package org.apache.rocketmq.common.action;

import org.apache.rocketmq.common.resource.ResourceType;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class RocketMQActionTest {

    @Test
    public void testRocketMQAction_DefaultResourceType_CustomisedValueAndActionArray() {
        RocketMQAction annotation = DemoClass.class.getAnnotation(RocketMQAction.class);
        assertEquals(0, annotation.value());
        assertEquals(ResourceType.UNKNOWN, annotation.resource());
        assertArrayEquals(new Action[] {}, annotation.action());
    }

    @Test
    public void testRocketMQAction_CustomisedValueAndResourceTypeAndActionArray() {
        RocketMQAction annotation = CustomisedDemoClass.class.getAnnotation(RocketMQAction.class);
        assertEquals(1, annotation.value());
        assertEquals(ResourceType.TOPIC, annotation.resource());
        assertArrayEquals(new Action[] {Action.CREATE, Action.DELETE}, annotation.action());
    }

    @RocketMQAction(value = 0, resource = ResourceType.UNKNOWN, action = {})
    private static class DemoClass {
    }

    @RocketMQAction(value = 1, resource = ResourceType.TOPIC, action = {Action.CREATE, Action.DELETE})
    private static class CustomisedDemoClass {
    }
}
