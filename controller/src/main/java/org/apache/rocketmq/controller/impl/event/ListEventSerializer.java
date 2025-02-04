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
import org.apache.rocketmq.common.utils.Serializer;
import org.apache.rocketmq.logging.org.slf4j.Logger;

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.List;

public class ListEventSerializer {
    private ListEventSerializer() {
    }

    private static final Serializer SERIALIZER = new FastJsonSerializer();

    private static void putShort(byte[] memory, int index, int value) {
        memory[index] = (byte) (value >>> 8);
        memory[index + 1] = (byte) value;
    }

    private static void putShort(ByteArrayOutputStream outputStream, int value) {
        outputStream.write((byte) (value >>> 8));
        outputStream.write((byte) value);
    }

    private static short getShort(byte[] memory, int index) {
        return (short) (memory[index] << 8 | memory[index + 1] & 0xFF);
    }

    private static void putInt(byte[] memory, int index, int value) {
        memory[index] = (byte) (value >>> 24);
        memory[index + 1] = (byte) (value >>> 16);
        memory[index + 2] = (byte) (value >>> 8);
        memory[index + 3] = (byte) value;
    }

    private static void putInt(ByteArrayOutputStream outputStream, int value) {
        outputStream.write((byte) (value >>> 24));
        outputStream.write((byte) (value >>> 16));
        outputStream.write((byte) (value >>> 8));
        outputStream.write((byte) value);
    }

    private static int getInt(byte[] memory, int index) {
        return memory[index] << 24 | (memory[index + 1] & 0xFF) << 16 | (memory[index + 2] & 0xFF) << 8 | memory[index + 3] & 0xFF;
    }

    public static byte[] serialize(List<EventMessage> message, Logger log) throws SerializationException {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        for (EventMessage eventMessage : message) {
            final short eventType = eventMessage.getEventType().getId();
            final byte[] data = SERIALIZER.serialize(eventMessage);
            if (data != null && data.length > 0) {
                putShort(outputStream, eventType);
                putInt(outputStream, data.length);
                outputStream.write(data, 0, data.length);
            } else {
                log.error("serialize event message error, event: {}, this event will be discard", eventMessage);
            }
        }
        return outputStream.toByteArray();
    }

    public static List<EventMessage> deserialize(byte[] bytes, Logger log) throws SerializationException {
        List<EventMessage> eventMessages = new ArrayList<>();
        if (bytes == null || bytes.length <= 6) {
            return eventMessages;
        }
        int index = 0;
        while (index < bytes.length) {
            final short eventId = getShort(bytes, index);
            index += 2;
            final int dataLength = getInt(bytes, index);
            index += 4;
            if (dataLength > 0) {
                final byte[] data = new byte[dataLength];
                System.arraycopy(bytes, index, data, 0, dataLength);
                final EventType eventType = EventType.from(eventId);
                if (eventType != null) {
                    switch (eventType) {
                        case ALTER_SYNC_STATE_SET_EVENT:
                            eventMessages.add(SERIALIZER.deserialize(data, AlterSyncStateSetEvent.class));
                            break;
                        case APPLY_BROKER_ID_EVENT:
                            eventMessages.add(SERIALIZER.deserialize(data, ApplyBrokerIdEvent.class));
                            break;
                        case ELECT_MASTER_EVENT:
                            eventMessages.add(SERIALIZER.deserialize(data, ElectMasterEvent.class));
                            break;
                        case CLEAN_BROKER_DATA_EVENT:
                            eventMessages.add(SERIALIZER.deserialize(data, CleanBrokerDataEvent.class));
                            break;
                        case UPDATE_BROKER_ADDRESS:
                            eventMessages.add(SERIALIZER.deserialize(data, UpdateBrokerAddressEvent.class));
                            break;
                        default:
                            log.error("deserialize event message error, event id: {}, data: {}", eventId, data);
                            break;
                    }
                } else {
                    log.error("deserialize event message error, event id: {}, data: {}", eventId, data);
                }
                index += dataLength;
            } else {
                log.error("deserialize event message error, event id: {}, data length: {}", eventId, dataLength);
            }
        }
        return eventMessages;
    }
}
