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
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class ListEventSerializerTest {

    @Mock
    private Logger logger;

    @Test
    public void testSerializeEmptyList() {
        List<EventMessage> events = Collections.emptyList();
        byte[] result = ListEventSerializer.serialize(events, null);
        assertNotNull(result);
        assertEquals(0, result.length);
    }

    @Test
    public void testSerializeValidEventMessage() {
        EventMessage eventMessage = new ElectMasterEvent("brokerA", 0L);
        List<EventMessage> events = Collections.singletonList(eventMessage);
        byte[] result = ListEventSerializer.serialize(events, null);
        assertNotNull(result);
        assertTrue(result.length > 0);
    }

    @Test
    public void testSerializeEventMessageWithNullEventType() {
        EventMessage eventMessage = mock(EventMessage.class);
        when(eventMessage.getEventType()).thenReturn(null);
        List<EventMessage> events = Collections.singletonList(eventMessage);
        assertThrows(NullPointerException.class, () -> ListEventSerializer.serialize(events, logger));
    }

    @Test
    public void testDeserializeBytesIsNull() throws SerializationException {
        List<EventMessage> result = ListEventSerializer.deserialize(null, logger);
        assertNotNull(result);
        assertTrue(result.isEmpty());
    }

    @Test
    public void testDeserializeBytesLengthLessThanSix() throws SerializationException {
        byte[] bytes = new byte[5];
        List<EventMessage> result = ListEventSerializer.deserialize(bytes, logger);
        assertNotNull(result);
        assertTrue(result.isEmpty());
    }

    @Test
    public void testDeserializeValidBytesWithKnownEventType() throws SerializationException {
        byte[] bytes = new byte[]{0x01, 0x00, 0x06, 0x00, 0x00, 0x00};
        assertNotNull(ListEventSerializer.deserialize(bytes, logger));
    }

    @Test
    public void testDeserializeException() throws SerializationException {
        byte[] bytes = new byte[]{0x01, 0x00, 0x06, 0x00, 0x00, 0x00, 0x02, 0x00, 0x06, 0x00, 0x00, 0x00};
        assertThrows(ArrayIndexOutOfBoundsException.class, () -> ListEventSerializer.deserialize(bytes, logger));
    }
}
