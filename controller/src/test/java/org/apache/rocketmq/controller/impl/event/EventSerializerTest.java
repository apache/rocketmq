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
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.rocketmq.common.utils.FastJsonSerializer;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class EventSerializerTest {

    @Mock
    private FastJsonSerializer serializer;

    private final EventSerializer eventSerializer = new EventSerializer();

    @Before
    public void init() throws IllegalAccessException {
        FieldUtils.writeDeclaredField(eventSerializer, "serializer", serializer, true);
    }

    @Test
    public void testSerializeValidEventMessageShouldReturnSerializedData() {
        EventMessage eventMessage = mock(EventMessage.class);
        EventType eventType = EventType.APPLY_BROKER_ID_EVENT;
        when(eventMessage.getEventType()).thenReturn(eventType);
        when(serializer.serialize(eventMessage)).thenReturn("{\"event\":\"APPLY_BROKER_ID_EVENT\"}".getBytes());
        byte[] result = eventSerializer.serialize(eventMessage);
        assertNotNull(result);
    }

    @Test
    public void testSerializeEventMessageWithNoEventType() {
        EventMessage eventMessage = mock(EventMessage.class);
        when(eventMessage.getEventType()).thenReturn(null);
        assertThrows(NullPointerException.class, () -> eventSerializer.serialize(eventMessage));
    }

    @Test
    public void testSerializeSerializerReturnsNullShouldReturnNull() {
        EventMessage eventMessage = mock(EventMessage.class);
        EventType eventType = EventType.READ_EVENT;
        when(eventMessage.getEventType()).thenReturn(eventType);
        when(serializer.serialize(eventMessage)).thenReturn(null);
        byte[] result = eventSerializer.serialize(eventMessage);
        assertNull(result);
    }

    @Test
    public void testSerializeSerializerThrowsException() {
        EventMessage eventMessage = mock(EventMessage.class);
        EventType eventType = EventType.ELECT_MASTER_EVENT;
        when(eventMessage.getEventType()).thenReturn(eventType);
        when(serializer.serialize(eventMessage)).thenThrow(new RuntimeException("Serialization error"));
        assertThrows(RuntimeException.class, () -> eventSerializer.serialize(eventMessage));
    }

    @Test
    public void testDeserializeBytesLessThanTwoReturnsNull() {
        byte[] bytes = new byte[1];
        assertNull(eventSerializer.deserialize(bytes));
    }

    @Test
    public void testDeserializeInvalidEventIdReturnsNull() {
        assertNull(eventSerializer.deserialize(new byte[]{0, 0xF}));
    }

    @Test
    public void testDeserializeValidEventTypeReturnsEventMessage() throws SerializationException {
        byte[] data = new byte[]{0, 0xF};
        byte[] bytes = new byte[]{0, (byte) EventType.ALTER_SYNC_STATE_SET_EVENT.getId(), data[0], data[1]};
        AlterSyncStateSetEvent alterSyncStateSetEvent = mock(AlterSyncStateSetEvent.class);
        when(serializer.deserialize(any(byte[].class), eq(AlterSyncStateSetEvent.class))).thenReturn(alterSyncStateSetEvent);
        EventMessage result = eventSerializer.deserialize(bytes);
        assertNotNull(result);
        assertTrue(result instanceof AlterSyncStateSetEvent);
    }

    @Test
    public void testDeserializeSerializerThrowsException() throws SerializationException {
        byte[] data = new byte[]{0, 0xF};
        byte[] bytes = new byte[]{0, (byte) EventType.ALTER_SYNC_STATE_SET_EVENT.getId(), data[0], data[1]};
        when(serializer.deserialize(any(byte[].class), eq(AlterSyncStateSetEvent.class))).thenThrow(new SerializationException("Deserialization failed"));
        assertThrows(SerializationException.class, () -> eventSerializer.deserialize(bytes));
    }

    @Test
    public void testDeserializeValidEventTypeUnknownEventReturnsNull() throws SerializationException {
        byte[] data = new byte[]{0, 0xF};
        byte[] bytes = new byte[]{0, (short) 99, data[0], data[1]};
        assertNull(eventSerializer.deserialize(bytes));
    }
}