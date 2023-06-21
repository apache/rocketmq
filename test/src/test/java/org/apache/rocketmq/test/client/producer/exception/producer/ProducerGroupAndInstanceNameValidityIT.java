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

import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.test.base.BaseConf;
import org.apache.rocketmq.test.client.rmq.RMQNormalProducer;
import org.apache.rocketmq.test.util.RandomUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static com.google.common.truth.Truth.assertThat;

public class ProducerGroupAndInstanceNameValidityIT extends BaseConf {
    private static Logger logger = LoggerFactory.getLogger(ProducerGroupAndInstanceNameValidityIT.class);
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
    public void testTwoProducerSameGroupAndInstanceName() {
        RMQNormalProducer producer1 = getProducer(NAMESRV_ADDR, topic);
        assertThat(producer1.isStartSuccess()).isEqualTo(true);
        RMQNormalProducer producer2 = getProducer(NAMESRV_ADDR, topic,
            producer1.getProducerGroupName(), producer1.getProducerInstanceName());
        assertThat(producer2.isStartSuccess()).isEqualTo(false);
    }

    /**
     * @since version3.4.6
     */
    @Test
    public void testTwoProducerSameGroup() {
        RMQNormalProducer producer1 = getProducer(NAMESRV_ADDR, topic);
        assertThat(producer1.isStartSuccess()).isEqualTo(true);
        RMQNormalProducer producer2 = getProducer(NAMESRV_ADDR, topic,
            producer1.getProducerGroupName(), RandomUtils.getStringByUUID());
        assertThat(producer2.isStartSuccess()).isEqualTo(true);
    }

}
