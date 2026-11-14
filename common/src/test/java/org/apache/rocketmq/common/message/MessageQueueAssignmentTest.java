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
package org.apache.rocketmq.common.message;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class MessageQueueAssignmentTest {

    @Test
    public void testEqualsHandlesNullMessageQueue() {
        assertThat(new MessageQueueAssignment()).isEqualTo(new MessageQueueAssignment());
    }

    @Test
    public void testEqualsAndHashCodeUseAllFields() {
        MessageQueue messageQueue = new MessageQueue("topic", "broker", 0);
        MessageQueueAssignment pull = new MessageQueueAssignment();
        pull.setMessageQueue(messageQueue);
        pull.setMode(MessageRequestMode.PULL);

        MessageQueueAssignment pop = new MessageQueueAssignment();
        pop.setMessageQueue(messageQueue);
        pop.setMode(MessageRequestMode.POP);

        assertThat(pull).isNotEqualTo(pop);

        MessageQueueAssignment samePull = new MessageQueueAssignment();
        samePull.setMessageQueue(messageQueue);
        samePull.setMode(MessageRequestMode.PULL);

        assertThat(pull).isEqualTo(samePull);
        assertThat(pull).hasSameHashCodeAs(samePull);
    }
}
