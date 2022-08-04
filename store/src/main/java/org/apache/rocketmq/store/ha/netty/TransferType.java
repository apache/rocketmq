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

package org.apache.rocketmq.store.ha.netty;

public enum TransferType {

    UNKNOWN(0),

    /**
     * Request to add a broker as slave
     * format: protocol version + language + brokerName + brokerId + brokerPerm
     */
    HANDSHAKE_SLAVE(1),

    /**
     * Master return the result of handshake
     * format: protocol version + language + brokerName + brokerId + brokerPerm
     */
    HANDSHAKE_MASTER(2),

    /**
     * query master epoch
     */
    QUERY_EPOCH(3),

    /**
     * master broker reply epoch
     */
    RETURN_EPOCH(4),

    /**
     * slave broker truncate self log and send signal to master, then master push data
     */
    CONFIRM_TRUNCATE(5),

    /**
     * Master broker transfer commitlog data to slave broker
     * format: current epoch, confirm offset, pull from offset, block size, content(large)
     */
    TRANSFER_DATA(6),

    /**
     * Slave broker report receive offset to master
     */
    TRANSFER_ACK(7);

    private final int value;

    TransferType(int value) {
        this.value = value;
    }

    public static TransferType valueOf(int code) {
        for (TransferType tmp : TransferType.values()) {
            if (tmp.getValue() == code) {
                return tmp;
            }
        }
        return UNKNOWN;
    }

    public int getValue() {
        return value;
    }
}
