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

package org.apache.rocketmq.controller.dledger.statemachine.event.write;


public enum WriteEventType {
    APPLY_BROKER_ID("ApplyBrokerIdEvent", (short) 1),
    REGISTER_BROKER("RegisterBrokerEvent", (short) 2),
    ELECT_MASTER("ElectMasterEvent", (short) 3),
    ALTER_SYNC_STATE_SET("AlterSyncStateSetEvent", (short) 4),
    CLEAN_BROKER_DATA("CleanBrokerDataEvent", (short) 5),

    UNKNOWN("UnknownEvent", (short) -1);

    private final String name;

    private final short id;


    WriteEventType(String name, short id) {
        this.name = name;
        this.id = id;
    }

    public static WriteEventType valueOf(short id) {
        switch (id) {
            case 1:
                return APPLY_BROKER_ID;
            case 2:
                return REGISTER_BROKER;
            case 3:
                return ELECT_MASTER;
            case 4:
                return ALTER_SYNC_STATE_SET;
            case 5:
                return CLEAN_BROKER_DATA;
        }
        return UNKNOWN;
    }

    public short getId() {
        return id;
    }

    public String getName() {
        return name;
    }
}
