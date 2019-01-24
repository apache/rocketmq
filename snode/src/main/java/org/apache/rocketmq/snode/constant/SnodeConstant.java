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
package org.apache.rocketmq.snode.constant;

import io.netty.util.AttributeKey;
import org.apache.rocketmq.snode.client.Client;
import org.apache.rocketmq.snode.client.impl.ClientRole;

public class SnodeConstant {
    public static final long HEARTBEAT_TIME_OUT = 3000;

    public static final long ONE_WAY_TIMEOUT = 10;

    public static final long DEFAULT_TIMEOUT_MILLS = 3000L;

    public static final long CONSUMER_TIMEOUT_MILLIS_WHEN_SUSPEND = 1000 * 30;

    public static final AttributeKey<ClientRole> NETTY_CLIENT_ROLE_ATTRIBUTE_KEY = AttributeKey.valueOf("netty.client.role");

    public static final AttributeKey<Client> NETTY_CLIENT_ATTRIBUTE_KEY = AttributeKey.valueOf("netty.client");
}
