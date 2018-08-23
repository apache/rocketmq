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
package io.openmessaging.rocketmq.utils;

import io.openmessaging.KeyValue;
import io.openmessaging.OMS;
import io.openmessaging.rocketmq.config.ClientConfig;
import io.openmessaging.rocketmq.domain.NonStandardKeys;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class BeanUtilsTest {
    private KeyValue properties = OMS.newKeyValue();

    public static class CustomizedConfig extends ClientConfig {
        final static String STRING_TEST = "string.test";
        String stringTest = "foobar";

        final static String DOUBLE_TEST = "double.test";
        double doubleTest = 123.0;

        final static String LONG_TEST = "long.test";
        long longTest = 123L;

        String getStringTest() {
            return stringTest;
        }

        public void setStringTest(String stringTest) {
            this.stringTest = stringTest;
        }

        double getDoubleTest() {
            return doubleTest;
        }

        public void setDoubleTest(final double doubleTest) {
            this.doubleTest = doubleTest;
        }

        long getLongTest() {
            return longTest;
        }

        public void setLongTest(final long longTest) {
            this.longTest = longTest;
        }

        CustomizedConfig() {
        }
    }

    @Before
    public void init() {
        properties.put(NonStandardKeys.MAX_REDELIVERY_TIMES, 120);
        properties.put(CustomizedConfig.STRING_TEST, "kaka");
        properties.put(NonStandardKeys.CONSUMER_GROUP, "Default_Consumer_Group");
        properties.put(NonStandardKeys.MESSAGE_CONSUME_TIMEOUT, 101);

        properties.put(CustomizedConfig.LONG_TEST, 1234567890L);
        properties.put(CustomizedConfig.DOUBLE_TEST, 10.234);
    }

    @Test
    public void testPopulate() {
        CustomizedConfig config = BeanUtils.populate(properties, CustomizedConfig.class);

        //RemotingConfig config = BeanUtils.populate(properties, RemotingConfig.class);
        Assert.assertEquals(config.getRmqMaxRedeliveryTimes(), 120);
        Assert.assertEquals(config.getStringTest(), "kaka");
        Assert.assertEquals(config.getRmqConsumerGroup(), "Default_Consumer_Group");
        Assert.assertEquals(config.getRmqMessageConsumeTimeout(), 101);
        Assert.assertEquals(config.getLongTest(), 1234567890L);
        Assert.assertEquals(config.getDoubleTest(), 10.234, 0.000001);
    }

    @Test
    public void testPopulate_ExistObj() {
        CustomizedConfig config = new CustomizedConfig();
        config.setConsumerId("NewConsumerId");

        Assert.assertEquals(config.getConsumerId(), "NewConsumerId");

        config = BeanUtils.populate(properties, config);

        //RemotingConfig config = BeanUtils.populate(properties, RemotingConfig.class);
        Assert.assertEquals(config.getRmqMaxRedeliveryTimes(), 120);
        Assert.assertEquals(config.getStringTest(), "kaka");
        Assert.assertEquals(config.getRmqConsumerGroup(), "Default_Consumer_Group");
        Assert.assertEquals(config.getRmqMessageConsumeTimeout(), 101);
        Assert.assertEquals(config.getLongTest(), 1234567890L);
        Assert.assertEquals(config.getDoubleTest(), 10.234, 0.000001);
    }

}