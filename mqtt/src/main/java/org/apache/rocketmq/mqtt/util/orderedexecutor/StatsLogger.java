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
 * A simple interface that exposes just 2 useful methods. One to get the logger for an Op stat
 * and another to get the logger for a simple stat
 */
public interface StatsLogger {
    /**
     * @param name
     *          Stats Name
     * @return Get the logger for an OpStat described by the <i>name</i>.
     */
    OpStatsLogger getOpStatsLogger(String name);

    /**
     * @param name
     *          Stats Name
     * @return Get the logger for a simple stat described by the <i>name</i>
     */
    Counter getCounter(String name);

    /**
     * Register given <i>gauge</i> as name <i>name</i>.
     *
     * @param name
     *          gauge name
     * @param gauge
     *          gauge function
     */
    <T extends Number> void registerGauge(String name, Gauge<T> gauge);

    /**
     * Unregister given <i>gauge</i> from name <i>name</i>.
     *
     * @param name
     *          name of the gauge
     * @param gauge
     *          gauge function
     */
    <T extends Number> void unregisterGauge(String name, Gauge<T> gauge);

    /**
     * Provide the stats logger under scope <i>name</i>.
     *
     * @param name
     *          scope name.
     * @return stats logger under scope <i>name</i>.
     */
    StatsLogger scope(String name);

    /**
     * Remove the given <i>statsLogger</i> for scope <i>name</i>.
     * It can be no-op if the underlying stats provider doesn't have the ability to remove scope.
     *
     * @param name name of the scope
     * @param statsLogger the stats logger of this scope.
     */
    void removeScope(String name, StatsLogger statsLogger);

}
