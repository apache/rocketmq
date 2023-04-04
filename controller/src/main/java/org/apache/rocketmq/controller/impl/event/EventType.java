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
package org.apache.rocketmq.controller.impl.event;

/**
 * Event type (name, id);
 */
public enum EventType {
    ALTER_SYNC_STATE_SET_EVENT("AlterSyncStateSetEvent", (short) 1),
    APPLY_BROKER_ID_EVENT("ApplyBrokerIdEvent", (short) 2),
    ELECT_MASTER_EVENT("ElectMasterEvent", (short) 3),
    READ_EVENT("ReadEvent", (short) 4),
    CLEAN_BROKER_DATA_EVENT("CleanBrokerDataEvent", (short) 5),

    UPDATE_BROKER_ADDRESS("UpdateBrokerAddressEvent", (short) 6);

    private final String name;
    private final short id;

    EventType(String name, short id) {
        this.name = name;
        this.id = id;
    }

    public static EventType from(short id) {
        switch (id) {
            case 1:
                return ALTER_SYNC_STATE_SET_EVENT;
            case 2:
                return APPLY_BROKER_ID_EVENT;
            case 3:
                return ELECT_MASTER_EVENT;
            case 4:
                return READ_EVENT;
            case 5:
                return CLEAN_BROKER_DATA_EVENT;
            case 6:
                return UPDATE_BROKER_ADDRESS;
        }
        return null;
    }

    public String getName() {
        return name;
    }

    public short getId() {
        return id;
    }
}
