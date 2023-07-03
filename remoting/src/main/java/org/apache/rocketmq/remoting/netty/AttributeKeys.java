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
package org.apache.rocketmq.remoting.netty;


import io.netty.util.AttributeKey;
import org.apache.rocketmq.common.constant.HAProxyConstants;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class AttributeKeys {

    public static final AttributeKey<String> PROXY_PROTOCOL_ADDR =
            AttributeKey.valueOf(HAProxyConstants.PROXY_PROTOCOL_ADDR);

    public static final AttributeKey<String> PROXY_PROTOCOL_PORT =
            AttributeKey.valueOf(HAProxyConstants.PROXY_PROTOCOL_PORT);

    public static final AttributeKey<String> PROXY_PROTOCOL_SERVER_ADDR =
            AttributeKey.valueOf(HAProxyConstants.PROXY_PROTOCOL_SERVER_ADDR);

    public static final AttributeKey<String> PROXY_PROTOCOL_SERVER_PORT =
            AttributeKey.valueOf(HAProxyConstants.PROXY_PROTOCOL_SERVER_PORT);

    private static final Map<String, AttributeKey<String>> ATTRIBUTE_KEY_MAP = new ConcurrentHashMap<>();

    public static AttributeKey<String> valueOf(String name) {
        return ATTRIBUTE_KEY_MAP.computeIfAbsent(name, AttributeKeys::valueOf);
    }
}
