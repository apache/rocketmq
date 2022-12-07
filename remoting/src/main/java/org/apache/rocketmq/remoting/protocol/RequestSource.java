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
package org.apache.rocketmq.remoting.protocol;

public enum RequestSource {

    SDK(-1),
    PROXY_FOR_ORDER(0),
    PROXY_FOR_BROADCAST(1),
    PROXY_FOR_STREAM(2);

    public static final String SYSTEM_PROPERTY_KEY = "rocketmq.requestSource";
    private final int value;

    RequestSource(int value) {
        this.value = value;
    }

    public int getValue() {
        return value;
    }

    public static boolean isValid(Integer value) {
        return null != value && value >= -1 && value < RequestSource.values().length - 1;
    }

    public static RequestSource parseInteger(Integer value) {
        if (isValid(value)) {
            return RequestSource.values()[value + 1];
        }
        return SDK;
    }
}
