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
package org.apache.rocketmq.logappender;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.Configurator;
import org.apache.rocketmq.client.exception.MQClientException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class log4j2Test extends AbstractTestCase {

    @Before
    public void init() {
        Configurator.initialize("log4j2", "src/test/resources/log4j2-example.xml");
    }

    @Test
    public void testLog4j2() throws InterruptedException, MQClientException {
        clear();
        Logger logger = LogManager.getLogger("test");
        for (int i = 0; i < 10; i++) {
            logger.info("log4j2 log message " + i);
        }
        int received = consumeMessages(10, "log4j2", 10);
        Assert.assertTrue(received > 5);
    }
}
