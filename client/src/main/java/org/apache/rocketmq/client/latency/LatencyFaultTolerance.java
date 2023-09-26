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

package org.apache.rocketmq.client.latency;

public interface LatencyFaultTolerance<T> {
    /**
     * Update brokers' states, to decide if they are good or not.
     *
     * @param name Broker's name.
     * @param currentLatency Current message sending process's latency.
     * @param notAvailableDuration Corresponding not available time, ms. The broker will be not available until it
     * spends such time.
     * @param reachable To decide if this broker is reachable or not.
     */
    void updateFaultItem(final T name, final long currentLatency, final long notAvailableDuration,
                         final boolean reachable);

    /**
     * To check if this broker is available.
     *
     * @param name Broker's name.
     * @return boolean variable, if this is true, then the broker is available.
     */
    boolean isAvailable(final T name);

    /**
     * To check if this broker is reachable.
     *
     * @param name Broker's name.
     * @return boolean variable, if this is true, then the broker is reachable.
     */
    boolean isReachable(final T name);

    /**
     * Remove the broker in this fault item table.
     *
     * @param name broker's name.
     */
    void remove(final T name);

    /**
     * The worst situation, no broker can be available. Then choose random one.
     *
     * @return A random mq will be returned.
     */
    T pickOneAtLeast();

    /**
     * Start a new thread, to detect the broker's reachable tag.
     */
    void startDetector();

    /**
     * Shutdown threads that started by LatencyFaultTolerance.
     */
    void shutdown();

    /**
     * A function reserved, just detect by once, won't create a new thread.
     */
    void detectByOneRound();

    /**
     * Use it to set the detect timeout bound.
     *
     * @param detectTimeout timeout bound
     */
    void setDetectTimeout(final int detectTimeout);

    /**
     * Use it to set the detector's detector interval for each broker (each broker will be detected once during this
     * time)
     *
     * @param detectInterval each broker's detecting interval
     */
    void setDetectInterval(final int detectInterval);

    /**
     * Use it to set the detector work or not.
     *
     * @param startDetectorEnable set the detector's work status
     */
    void setStartDetectorEnable(final boolean startDetectorEnable);

    /**
     * Use it to judge if the detector enabled.
     *
     * @return is the detector should be started.
     */
    boolean isStartDetectorEnable();
}
