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
package org.apache.rocketmq.store.config;

/**
 * define BrokerRole enum class
 */
public enum BrokerRole {
    /**
     * When the slave and master messages are completed asynchronously,
     * there is no need to send a successful status
     */
    ASYNC_MASTER,
    /**
     * When the slave and master messages are synchronized,
     * they will return to the status of successful sending
     */
    SYNC_MASTER,
    /**
     * Indicates the slave, used to read the message
     */
    SLAVE;
}
