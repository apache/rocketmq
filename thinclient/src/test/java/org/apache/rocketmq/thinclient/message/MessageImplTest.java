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

package org.apache.rocketmq.thinclient.message;

import java.util.Arrays;
import org.apache.rocketmq.apis.ClientServiceProvider;
import org.apache.rocketmq.apis.message.Message;
import org.apache.rocketmq.thinclient.tool.TestBase;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class MessageImplTest extends TestBase {
    private final ClientServiceProvider provider = ClientServiceProvider.loadService();
    private final String sampleTopic = "foobar";
    private final byte[] sampleBody = new byte[] {'f', 'o', 'o'};

    @Test(expected = NullPointerException.class)
    public void testTopicSetterWithNull() {
        provider.newMessageBuilder().setTopic(null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testTopicSetterWithLengthEquals128() {
        String topicWithLengthEquals128 = new String(new char[128]).replace("\0", "a");
        provider.newMessageBuilder().setTopic(topicWithLengthEquals128);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testTagSetterWithVerticalBar() {
        provider.newMessageBuilder().setTag("|");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testTagSetterWithBlank() {
        provider.newMessageBuilder().setTag("\t");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testTagSetterWithMixedBlank() {
        provider.newMessageBuilder().setTag(" \n");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testTagSetterWithSpaces() {
        provider.newMessageBuilder().setTag("  ");
    }

    @Test
    public void testTagSetter() {
        final Message message = provider.newMessageBuilder().setTag("tagA").setTopic(FAKE_TOPIC_0).setBody(FAKE_MESSAGE_BODY).build();
        assertTrue(message.getTag().isPresent());
        assertEquals("tagA", message.getTag().get());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testKeySetterWithBlank() {
        provider.newMessageBuilder().setKeys("\t");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testKeySetterWithMixedBlank() {
        provider.newMessageBuilder().setKeys(" \n");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testKeySetterWithSpaces() {
        provider.newMessageBuilder().setKeys(" ");
    }

    @Test
    public void testKeySetter() {
        final Message message = provider.newMessageBuilder().setKeys("keyA").setTopic(FAKE_TOPIC_0).setBody(FAKE_MESSAGE_BODY).build();
        assertFalse(message.getKeys().isEmpty());
    }

    @Test
    public void testMessageBodySetterGetterImmutability() {
        byte[] body = sampleBody.clone();

        final Message message = provider.newMessageBuilder().setTopic(sampleTopic).setBody(body).build();
        // Modify message body set before.
        body[0] = 'g';
        Assert.assertEquals('g', body[0]);

        byte[] currentBody = new byte[message.getBody().remaining()];
        message.getBody().get(currentBody);
        Assert.assertEquals('f', currentBody[0]);
    }

    @Test
    public void testMessagePropertiesGetterImmutability() {
        byte[] body = sampleBody.clone();

        String propertyKey = "foo";
        String propertyValue = "value";
        Map<String, String> property = new HashMap<>();
        property.put(propertyKey, propertyValue);

        final Message message = provider.newMessageBuilder().setTopic(sampleTopic)
            .setBody(body).addProperty(propertyKey, propertyValue).build();
        Assert.assertEquals(property, message.getProperties());
        // Clear properties gotten.
        message.getProperties().clear();
        Assert.assertEquals(property, message.getProperties());
    }

    @Test
    public void testBuild() {
        final Message message = provider.newMessageBuilder().setTopic(FAKE_TOPIC_0).setBody(FAKE_MESSAGE_BODY).build();
        assertEquals(FAKE_TOPIC_0, message.getTopic());
        final byte[] bytes = new byte[message.getBody().remaining()];
        message.getBody().get(bytes);
        assertArrayEquals(FAKE_MESSAGE_BODY, bytes);
        assertFalse(message.getDeliveryTimestamp().isPresent());
        assertFalse(message.getMessageGroup().isPresent());
        assertFalse(message.getParentTraceContext().isPresent());
    }
}