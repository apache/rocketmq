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

import org.apache.log4j.Logger;
import org.apache.rocketmq.client.exception.MQClientException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public abstract class Log4jTest extends AbstractTestCase {

    @Before
    public abstract void init();

    public abstract String getType();

    @Test
    public void testLog4j() throws InterruptedException, MQClientException {
        clear();
        Logger logger = Logger.getLogger("testLogger");
        for (int i = 0; i < 10; i++) {
            logger.info("log4j " + this.getType() + " simple test message " + i);
        }
        int received = consumeMessages(10, "log4j", 10);
        Assert.assertTrue(received > 5);
    }

}
