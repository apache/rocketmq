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
package org.apache.rocketmq.tools.admin.common;

public enum AdminToolsResultCodeEnum {

    /**
     *
     */
    SUCCESS(200),

    REMOTING_ERROR(-1001),
    MQ_BROKER_ERROR(-1002),
    MQ_CLIENT_ERROR(-1003),
    INTERRUPT_ERROR(-1004),

    TOPIC_ROUTE_INFO_NOT_EXIST(-2001),
    CONSUMER_NOT_ONLINE(-2002);

    private int code;

    AdminToolsResultCodeEnum(int code) {
        this.code = code;
    }

    public int getCode() {
        return code;
    }
}
