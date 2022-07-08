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

import java.nio.channels.SocketChannel;

public interface HAConnection {
    /**
     * Start HA Connection
     */
    void start();

    /**
     * Shutdown HA Connection
     */
    void shutdown();

    /**
     * Close HA Connection
     */
    void close();

    /**
     * Get socket channel
     */
    SocketChannel getSocketChannel();

    /**
     * Get current state for ha connection
     *
     * @return HAConnectionState
     */
    HAConnectionState getCurrentState();

    /**
     * Get client address for ha connection
     *
     * @return client ip address
     */
    String getClientAddress();

    /**
     * Get the transfer rate per second
     *
     *  @return transfer bytes in second
     */
    long getTransferredByteInSecond();

    /**
     * Get the current transfer offset to the slave
     *
     * @return the current transfer offset to the slave
     */
    long getTransferFromWhere();

    /**
     * Get slave ack offset
     *
     * @return slave ack offset
     */
    long getSlaveAckOffset();
}
