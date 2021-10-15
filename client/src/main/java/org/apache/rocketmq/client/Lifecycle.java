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
package org.apache.rocketmq.client;

import org.apache.rocketmq.client.exception.MQClientException;

public interface Lifecycle {
    /**
     * Used for startup or initialization of a service endpoint. A service endpoint instance will be in a ready state
     * after this method has been completed.
     */
    void start() throws MQClientException;

    /**
     * Notify a service instance of the end of its life cycle. Once this method completes, the service endpoint could be
     * destroyed and eligible for garbage collection.
     */
    void shutdown();
}
