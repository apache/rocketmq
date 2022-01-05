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

import org.apache.rocketmq.common.attribute.Attribute;
import org.apache.rocketmq.common.attribute.EnumAttribute;

import java.util.HashMap;
import java.util.Map;

import static com.google.common.collect.Sets.newHashSet;

public class TopicAttributes {
    public static final EnumAttribute QUEUE_TYPE = new EnumAttribute(
            "queue.type",
            false,
            newHashSet("BatchCQ", "SimpleCQ"),
            "SimpleCQ"
    );
    public static final Map<String, Attribute> ALL;

    static {
        ALL = new HashMap<>();
        ALL.put(QUEUE_TYPE.getName(), QUEUE_TYPE);
    }
}
