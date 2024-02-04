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

package org.apache.rocketmq.store.ha;

public interface HAClient {

    /**
     * Start HAClient
     */
    void start();

    /**
     * Shutdown HAClient
     */
    void shutdown();

    /**
     * Wakeup HAClient
     */
    void wakeup();

    /**
     * Update master address
     *
     * @param newAddress
     */
    void updateMasterAddress(String newAddress);

    /**
     * Update master ha address
     *
     * @param newAddress
     */
    void updateHaMasterAddress(String newAddress);

    /**
     * Get master address
     *
     * @return master address
     */
    String getMasterAddress();

    /**
     * Get master ha address
     *
     * @return master ha address
     */
    String getHaMasterAddress();

    /**
     * Get HAClient last read timestamp
     *
     * @return last read timestamp
     */
    long getLastReadTimestamp();

    /**
     * Get HAClient last write timestamp
     *
     * @return last write timestamp
     */
    long getLastWriteTimestamp();

    /**
     * Get current state for ha connection
     *
     * @return HAConnectionState
     */
    HAConnectionState getCurrentState();

    /**
     * Change the current state for ha connection for testing
     *
     * @param haConnectionState
     */
    void changeCurrentState(HAConnectionState haConnectionState);

    /**
     * Disconnecting from the master for testing
     */
    void closeMaster();

    /**
     * Get the transfer rate per second
     *
     *  @return transfer bytes in second
     */
    long getTransferredByteInSecond();
}
