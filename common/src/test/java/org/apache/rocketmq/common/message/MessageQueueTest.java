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

public class MessageQueueTest {

    @Test
    public void testCompareToHandlesQueueIdExtremes() {
        MessageQueue minimum = new MessageQueue("topic", "broker", Integer.MIN_VALUE);
        MessageQueue one = new MessageQueue("topic", "broker", 1);
        MessageQueue maximum = new MessageQueue("topic", "broker", Integer.MAX_VALUE);

        assertThat(minimum.compareTo(one)).isNegative();
        assertThat(one.compareTo(minimum)).isPositive();
        assertThat(maximum.compareTo(minimum)).isPositive();
        assertThat(minimum.compareTo(maximum)).isNegative();
    }

    @Test
    public void testCompareToReturnsZeroForEqualQueue() {
        MessageQueue first = new MessageQueue("topic", "broker", 1);
        MessageQueue second = new MessageQueue("topic", "broker", 1);

        assertThat(first.compareTo(second)).isZero();
    }
}
