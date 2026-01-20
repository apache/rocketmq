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
package org.apache.rocketmq.common;

import static com.google.common.collect.Sets.newHashSet;

import java.util.HashMap;
import java.util.Map;
import org.apache.rocketmq.common.attribute.Attribute;
import org.apache.rocketmq.common.attribute.BooleanAttribute;
import org.apache.rocketmq.common.attribute.EnumAttribute;
import org.apache.rocketmq.common.attribute.LongRangeAttribute;
import org.apache.rocketmq.common.attribute.StringAttribute;
import org.apache.rocketmq.common.attribute.LiteSubModel;

public class SubscriptionGroupAttributes {

    public static final Map<String, Attribute> ALL;
    public static final LongRangeAttribute PRIORITY_FACTOR_ATTRIBUTE = new LongRangeAttribute(
        "priority.factor",
        true,
        0, // disable priority mode
        100, // enable priority mode
        100
    );

    public static final StringAttribute LITE_BIND_TOPIC_ATTRIBUTE = new StringAttribute(
        "lite.bind.topic",
        true
    );

    public static final EnumAttribute LITE_SUB_MODEL_ATTRIBUTE = new EnumAttribute(
        "lite.sub.model",
        true,
        newHashSet(LiteSubModel.Shared.name(), LiteSubModel.Exclusive.name()),
        LiteSubModel.Shared.name()
    );

    public static final BooleanAttribute LITE_SUB_RESET_OFFSET_EXCLUSIVE_ATTRIBUTE = new BooleanAttribute(
        "lite.sub.reset.offset.exclusive",
        true,
        false
    );

    public static final BooleanAttribute LITE_SUB_RESET_OFFSET_UNSUBSCRIBE_ATTRIBUTE = new BooleanAttribute(
        "lite.sub.reset.offset.unsubscribe",
        true,
        false
    );

    /**
     * client-side lite subscription quota limit
     */
    public static final LongRangeAttribute LITE_SUB_CLIENT_QUOTA_ATTRIBUTE = new LongRangeAttribute(
        "lite.sub.client.quota",
        true,
        -1,
        Long.MAX_VALUE,
        2000
    );

    public static final LongRangeAttribute LITE_SUB_CLIENT_MAX_EVENT_COUNT = new LongRangeAttribute(
        "lite.sub.client.max.event.cnt",
        true,
        10,
        Long.MAX_VALUE,
        400
    );

    static {
        ALL = new HashMap<>();
        ALL.put(PRIORITY_FACTOR_ATTRIBUTE.getName(), PRIORITY_FACTOR_ATTRIBUTE);
        ALL.put(LITE_BIND_TOPIC_ATTRIBUTE.getName(), LITE_BIND_TOPIC_ATTRIBUTE);
        ALL.put(LITE_SUB_CLIENT_QUOTA_ATTRIBUTE.getName(), LITE_SUB_CLIENT_QUOTA_ATTRIBUTE);
        ALL.put(LITE_SUB_MODEL_ATTRIBUTE.getName(), LITE_SUB_MODEL_ATTRIBUTE);
        ALL.put(LITE_SUB_RESET_OFFSET_EXCLUSIVE_ATTRIBUTE.getName(), LITE_SUB_RESET_OFFSET_EXCLUSIVE_ATTRIBUTE);
        ALL.put(LITE_SUB_RESET_OFFSET_UNSUBSCRIBE_ATTRIBUTE.getName(), LITE_SUB_RESET_OFFSET_UNSUBSCRIBE_ATTRIBUTE);
        ALL.put(LITE_SUB_CLIENT_MAX_EVENT_COUNT.getName(), LITE_SUB_CLIENT_MAX_EVENT_COUNT);
    }
}
