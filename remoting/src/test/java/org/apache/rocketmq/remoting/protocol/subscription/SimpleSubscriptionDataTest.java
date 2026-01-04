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

package org.apache.rocketmq.remoting.protocol.subscription;

import com.google.common.collect.Sets;
import java.util.Set;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class SimpleSubscriptionDataTest {
    @Test
    public void testNotEqual() {
        String topic = "test-topic";
        String expressionType = "TAG";
        String expression1 = "test-expression-1";
        String expression2 = "test-expression-2";
        SimpleSubscriptionData simpleSubscriptionData1 = new SimpleSubscriptionData(topic, expressionType, expression1, 1);
        SimpleSubscriptionData simpleSubscriptionData2 = new SimpleSubscriptionData(topic, expressionType, expression2, 1);
        assertThat(simpleSubscriptionData1.equals(simpleSubscriptionData2)).isFalse();
    }

    @Test
    public void testEqual() {
        String topic = "test-topic";
        String expressionType = "TAG";
        String expression1 = "test-expression-1";
        String expression2 = "test-expression-1";
        SimpleSubscriptionData simpleSubscriptionData1 = new SimpleSubscriptionData(topic, expressionType, expression1, 1);
        SimpleSubscriptionData simpleSubscriptionData2 = new SimpleSubscriptionData(topic, expressionType, expression2, 1);
        assertThat(simpleSubscriptionData1.equals(simpleSubscriptionData2)).isTrue();
    }

    @Test
    public void testSetNotEqual() {
        String topic = "test-topic";
        String expressionType = "TAG";
        String expression1 = "test-expression-1";
        String expression2 = "test-expression-2";
        Set<SimpleSubscriptionData> set1 = Sets.newHashSet(new SimpleSubscriptionData(topic, expressionType, expression1, 1));
        Set<SimpleSubscriptionData> set2 = Sets.newHashSet(new SimpleSubscriptionData(topic, expressionType, expression2, 1));
        assertThat(set1.equals(set2)).isFalse();
    }

    @Test
    public void testSetEqual() {
        String topic = "test-topic";
        String expressionType = "TAG";
        String expression1 = "test-expression-1";
        String expression2 = "test-expression-1";
        Set<SimpleSubscriptionData> set1 = Sets.newHashSet(new SimpleSubscriptionData(topic, expressionType, expression1, 1));
        Set<SimpleSubscriptionData> set2 = Sets.newHashSet(new SimpleSubscriptionData(topic, expressionType, expression2, 1));
        assertThat(set1.equals(set2)).isTrue();
    }
}