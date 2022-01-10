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
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.rocketmq.test.client.producer.exception.producer;

import java.lang.reflect.Field;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.log4j.Logger;
import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.test.base.BaseConf;
import org.apache.rocketmq.test.client.rmq.RMQNormalProducer;
import org.apache.rocketmq.test.util.RandomUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static com.google.common.truth.Truth.assertThat;

public class ProducerGroupAndInstanceNameValidityIT extends BaseConf {
    private static Logger logger = Logger.getLogger(ProducerGroupAndInstanceNameValidityIT.class);
    private String topic = null;

    @Before
    public void setUp() {
        topic = initTopic();
        logger.info(String.format("use topic: %s !", topic));
    }

    @After
    public void tearDown() {
        super.shutdown();
    }

    /**
     * @since version3.4.6
     */
    @Test
    public void testTwoProducerSameGroupAndInstanceName() throws Exception {
        RMQNormalProducer producer1 = getProducer(nsAddr, topic);
        Assert.assertEquals(true, producer1.isStartSuccess());
        RMQNormalProducer producer2 = getProducer(nsAddr, topic,
            producer1.getProducerGroupName(), producer1.getProducerInstanceName());
        Assert.assertEquals(true, producer2.isStartSuccess());

        Field field = FieldUtils.getDeclaredField(MQClientInstance.class, "instanceIndex", true);
        int instanceIndex1 = (int) field.get(producer1.getProducer().getDefaultMQProducerImpl().getMqClientFactory());
        field = FieldUtils.getDeclaredField(MQClientInstance.class, "instanceIndex", true);
        int instanceIndex2 = (int) field.get(producer2.getProducer().getDefaultMQProducerImpl().getMqClientFactory());
        Assert.assertNotEquals(instanceIndex1, instanceIndex2);

        Assert.assertEquals(producer1.getProducer().getFactoryIndex(), producer2.getProducer().getFactoryIndex());
        Assert.assertEquals(producer1.getProducer().getNameSrvCode(), producer2.getProducer().getNameSrvCode());
        Assert.assertNotEquals(producer1.getProducer().getGroupCode(), producer2.getProducer().getGroupCode());
        Assert.assertEquals(producer1.getProducer().getInstanceName(), producer2.getProducer().getInstanceName());
    }

    /**
     * @since version3.4.6
     */
    @Test
    public void testTwoProducerSameGroup() throws Exception {
        RMQNormalProducer producer1 = getProducer(nsAddr, topic);
        Assert.assertEquals(true, producer1.isStartSuccess());
        RMQNormalProducer producer2 = getProducer(nsAddr, topic,
            producer1.getProducerGroupName(), RandomUtils.getStringByUUID());
        Assert.assertEquals(true, producer2.isStartSuccess());

        Field field = FieldUtils.getDeclaredField(MQClientInstance.class, "instanceIndex", true);
        int instanceIndex1 = (int) field.get(producer1.getProducer().getDefaultMQProducerImpl().getMqClientFactory());
        field = FieldUtils.getDeclaredField(MQClientInstance.class, "instanceIndex", true);
        int instanceIndex2 = (int) field.get(producer2.getProducer().getDefaultMQProducerImpl().getMqClientFactory());
        Assert.assertNotEquals(instanceIndex1, instanceIndex2);

        Assert.assertEquals(producer1.getProducer().getFactoryIndex(), producer2.getProducer().getFactoryIndex());
        Assert.assertEquals(producer1.getProducer().getNameSrvCode(), producer2.getProducer().getNameSrvCode());
        Assert.assertEquals(producer1.getProducer().getGroupCode(), producer2.getProducer().getGroupCode());
        Assert.assertNotEquals(producer1.getProducer().getInstanceName(), producer2.getProducer().getInstanceName());
    }

}
