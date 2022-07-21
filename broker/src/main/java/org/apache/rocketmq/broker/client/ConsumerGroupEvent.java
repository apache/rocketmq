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
package org.apache.rocketmq.broker.client;

public enum ConsumerGroupEvent {

    /**
     * Some consumers in the group are changed.
     */
    CHANGE,
    /**
     * The group of consumer is unregistered.
     */
    UNREGISTER,
    /**
     * The group of consumer is registered.
     */
    REGISTER,
    /**
     * The client of this consumer is new registered.
     */
    CLIENT_REGISTER,
    /**
     * The client of this consumer is unregistered.
     */
    CLIENT_UNREGISTER
}
