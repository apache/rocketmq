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

import org.apache.commons.lang3.SerializationException;
import org.apache.rocketmq.common.utils.FastJsonSerializer;

/**
 * EventMessage serializer
 */
public class EventSerializer {
    private final FastJsonSerializer serializer;

    public EventSerializer() {
        this.serializer = new FastJsonSerializer();
    }

    private void putShort(byte[] memory, int index, int value) {
        memory[index] = (byte) (value >>> 8);
        memory[index + 1] = (byte) value;
    }

    private short getShort(byte[] memory, int index) {
        return (short) (memory[index] << 8 | memory[index + 1] & 0xFF);
    }

    public byte[] serialize(EventMessage message) throws SerializationException {
        final short eventType = message.getEventType().getId();
        final byte[] data = this.serializer.serialize(message);
        if (data != null && data.length > 0) {
            final byte[] result = new byte[2 + data.length];
            putShort(result, 0, eventType);
            System.arraycopy(data, 0, result, 2, data.length);
            return result;
        }
        return null;
    }

    public EventMessage deserialize(byte[] bytes) throws SerializationException {
        if (bytes.length < 2) {
            return null;
        }
        final short eventId = getShort(bytes, 0);
        if (eventId > 0) {
            final byte[] data = new byte[bytes.length - 2];
            System.arraycopy(bytes, 2, data, 0, data.length);
            final EventType eventType = EventType.from(eventId);
            if (eventType != null) {
                switch (eventType) {
                    case ALTER_SYNC_STATE_SET_EVENT:
                        return this.serializer.deserialize(data, AlterSyncStateSetEvent.class);
                    case APPLY_BROKER_ID_EVENT:
                        return this.serializer.deserialize(data, ApplyBrokerIdEvent.class);
                    case ELECT_MASTER_EVENT:
                        return this.serializer.deserialize(data, ElectMasterEvent.class);
                    case CLEAN_BROKER_DATA_EVENT:
                        return this.serializer.deserialize(data, CleanBrokerDataEvent.class);
                    case UPDATE_BROKER_ADDRESS:
                        return this.serializer.deserialize(data, UpdateBrokerAddressEvent.class);
                    default:
                        break;
                }
            }
        }
        return null;
    }
}
