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

package org.apache.rocketmq.thinclient.impl.consumer;

import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.apis.ClientConfiguration;
import org.apache.rocketmq.apis.ClientException;
import org.apache.rocketmq.thinclient.tool.TestBase;
import org.junit.Test;

public class SimpleConsumerBuilderTest extends TestBase {

    @Test(expected = NullPointerException.class)
    public void testSetClientConfigurationWithNull() {
        final SimpleConsumerBuilderImpl builder = new SimpleConsumerBuilderImpl();
        builder.setClientConfiguration(null);
    }

    @Test(expected = NullPointerException.class)
    public void testSetConsumerGroupWithNull() {
        final SimpleConsumerBuilderImpl builder = new SimpleConsumerBuilderImpl();
        builder.setConsumerGroup(null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSetConsumerGroupWithTooLong() {
        final SimpleConsumerBuilderImpl builder = new SimpleConsumerBuilderImpl();
        String tooLongConsumerGroup = StringUtils.repeat("a", 256);
        builder.setConsumerGroup(tooLongConsumerGroup);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testBuildWithoutExpressions() throws ClientException {
        final SimpleConsumerBuilderImpl builder = new SimpleConsumerBuilderImpl();
        ClientConfiguration clientConfiguration = ClientConfiguration.newBuilder().setAccessPoint(FAKE_ACCESS_POINT).build();
        builder.setClientConfiguration(clientConfiguration).setConsumerGroup(FAKE_GROUP_0)
            .build();
    }
}
