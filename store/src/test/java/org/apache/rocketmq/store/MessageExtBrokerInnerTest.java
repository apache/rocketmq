/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.rocketmq.store;

import org.apache.rocketmq.common.message.MessageExtBrokerInner;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class MessageExtBrokerInnerTest {
    @Test
    public void testDeleteProperty() {
        MessageExtBrokerInner messageExtBrokerInner = new MessageExtBrokerInner();
        String propertiesString = "";
        messageExtBrokerInner.setPropertiesString(propertiesString);
        messageExtBrokerInner.deleteProperty("KeyA");
        assertThat(messageExtBrokerInner.getPropertiesString()).isEqualTo("");

        propertiesString = "KeyA\u0001ValueA";
        messageExtBrokerInner.setPropertiesString(propertiesString);
        messageExtBrokerInner.deleteProperty("KeyA");
        assertThat(messageExtBrokerInner.getPropertiesString()).isEqualTo("");

        propertiesString = "KeyA\u0001ValueA\u0002";
        messageExtBrokerInner.setPropertiesString(propertiesString);
        messageExtBrokerInner.deleteProperty("KeyA");
        assertThat(messageExtBrokerInner.getPropertiesString()).isEqualTo("");

        propertiesString = "KeyA\u0001ValueA\u0002KeyA\u0001ValueA";
        messageExtBrokerInner.setPropertiesString(propertiesString);
        messageExtBrokerInner.deleteProperty("KeyA");
        assertThat(messageExtBrokerInner.getPropertiesString()).isEqualTo("");

        propertiesString = "KeyA\u0001ValueA\u0002KeyA\u0001ValueA\u0002";
        messageExtBrokerInner.setPropertiesString(propertiesString);
        messageExtBrokerInner.deleteProperty("KeyA");
        assertThat(messageExtBrokerInner.getPropertiesString()).isEqualTo("");

        propertiesString = "KeyB\u0001ValueB\u0002KeyA\u0001ValueA";
        messageExtBrokerInner.setPropertiesString(propertiesString);
        messageExtBrokerInner.deleteProperty("KeyA");
        assertThat(messageExtBrokerInner.getPropertiesString()).isEqualTo("KeyB\u0001ValueB\u0002");

        propertiesString = "KeyB\u0001ValueB\u0002KeyA\u0001ValueA\u0002";
        messageExtBrokerInner.setPropertiesString(propertiesString);
        messageExtBrokerInner.deleteProperty("KeyA");
        assertThat(messageExtBrokerInner.getPropertiesString()).isEqualTo("KeyB\u0001ValueB\u0002");

        propertiesString = "KeyB\u0001ValueB\u0002KeyA\u0001ValueA\u0002KeyB\u0001ValueB\u0002";
        messageExtBrokerInner.setPropertiesString(propertiesString);
        messageExtBrokerInner.deleteProperty("KeyA");
        assertThat(messageExtBrokerInner.getPropertiesString()).isEqualTo("KeyB\u0001ValueB\u0002KeyB\u0001ValueB\u0002");

        propertiesString = "KeyA\u0001ValueA\u0002KeyB\u0001ValueB\u0002";
        messageExtBrokerInner.setPropertiesString(propertiesString);
        messageExtBrokerInner.deleteProperty("KeyA");
        assertThat(messageExtBrokerInner.getPropertiesString()).isEqualTo("KeyB\u0001ValueB\u0002");

        propertiesString = "KeyA\u0001ValueA\u0002KeyB\u0001ValueB";
        messageExtBrokerInner.setPropertiesString(propertiesString);
        messageExtBrokerInner.deleteProperty("KeyA");
        assertThat(messageExtBrokerInner.getPropertiesString()).isEqualTo("KeyB\u0001ValueB");

        propertiesString = "KeyA\u0001ValueA\u0002KeyB\u0001ValueBKeyA\u0001ValueA\u0002";
        messageExtBrokerInner.setPropertiesString(propertiesString);
        messageExtBrokerInner.deleteProperty("KeyA");
        assertThat(messageExtBrokerInner.getPropertiesString()).isEqualTo("KeyB\u0001ValueBKeyA\u0001ValueA\u0002");

        propertiesString = "KeyA\u0001ValueA\u0002KeyB\u0001ValueBKeyA\u0001";
        messageExtBrokerInner.setPropertiesString(propertiesString);
        messageExtBrokerInner.deleteProperty("KeyA");
        assertThat(messageExtBrokerInner.getPropertiesString()).isEqualTo("KeyB\u0001ValueBKeyA\u0001");

        propertiesString = "KeyA\u0001ValueA\u0002KeyB\u0001ValueBKeyA";
        messageExtBrokerInner.setPropertiesString(propertiesString);
        messageExtBrokerInner.deleteProperty("KeyA");
        assertThat(messageExtBrokerInner.getPropertiesString()).isEqualTo("KeyB\u0001ValueBKeyA");

        propertiesString = "__CRC32#\u0001";
        messageExtBrokerInner.setPropertiesString(propertiesString);
        messageExtBrokerInner.deleteProperty("__CRC32#");
        assertThat(messageExtBrokerInner.getPropertiesString()).isEmpty();

        propertiesString = "__CRC32#";
        messageExtBrokerInner.setPropertiesString(propertiesString);
        messageExtBrokerInner.deleteProperty("__CRC32#");
        assertThat(messageExtBrokerInner.getPropertiesString()).isEqualTo(propertiesString);
    }

}
