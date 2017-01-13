/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.apache.rocketmq.common.consumer;

/**
 * @date 201701/102
 */
public enum AcknowledgementMode {
    /**
     * update offset locally, and lazily ack to broker
     */
    DUPS_OK_ACKNOWLEDGE("DUPS_OK_ACKNOWLEDGE"),

    /**
     * ack to broker immediately
     */
    AUTO_CLIENT_ACKNOWLEDGE("AUTO_CLIENT_ACKNOWLEDGE"),

    /**
     * Neither update offset nor ack to broker
     */

    CLIENT_ACKNOWLEDGE("CLIENT_ACKNOWLEDGE");

    private final String name;
    private AcknowledgementMode(String name) {
        this.name = name;
    }

    @Override
    public String toString() {
        return name;
    }
}
