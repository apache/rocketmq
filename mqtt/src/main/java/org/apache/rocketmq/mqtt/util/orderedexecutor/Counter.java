/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.rocketmq.mqtt.util.orderedexecutor;

/**
 * Simple stats that require only increment and decrement
 * functions on a Long. Metrics like the number of topics, persist queue size
 * etc. should use this.
 */
public interface Counter {
    /**
     * Clear this stat.
     */
    void clear();

    /**
     * Increment the value associated with this stat.
     */
    void inc();

    /**
     * Decrement the value associated with this stat.
     */
    void dec();

    /**
     * Add delta to the value associated with this stat.
     * @param delta
     */
    void add(long delta);

    /**
     * Get the value associated with this stat.
     */
    Long get();
}
