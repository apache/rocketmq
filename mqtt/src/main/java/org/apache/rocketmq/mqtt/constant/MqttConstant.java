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

package org.apache.rocketmq.mqtt.constant;

import io.netty.util.AttributeKey;
import org.apache.rocketmq.common.client.Client;

public class MqttConstant {
    public static final int MAX_SUPPORTED_QOS = 0;
    public static final String SUBSCRIPTION_FLAG_PLUS = "+";
    public static final String SUBSCRIPTION_FLAG_SHARP = "#";
    public static final String SUBSCRIPTION_SEPARATOR = "/";
    public static final String TOPIC_CLIENTID_SEPARATOR = "@";
    public static final long DEFAULT_TIMEOUT_MILLS = 3000L;
    public static final int FLIGHT_BEFORE_RESEND_MS = 5_000;
    public static final String PROPERTY_MQTT_QOS = "PROPERTY_MQTT_QOS";
    public static final AttributeKey<Client> MQTT_CLIENT_ATTRIBUTE_KEY = AttributeKey.valueOf("mqtt.client");
    public static final String ENODE_NAME = "enodeName";
    public static final String PERSIST_SUBSCRIPTION_SUFFIX = "-sub";
    public static final String PERSIST_SNODEADDRESS_SUFFIX = "-sno";
    public static final String PERSIST_CLIENT_SUFFIX = "-cli";
}
