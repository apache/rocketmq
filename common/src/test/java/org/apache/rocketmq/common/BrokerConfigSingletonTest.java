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
package org.apache.rocketmq.common;

import org.junit.Assert;
import org.junit.Test;

public class BrokerConfigSingletonTest {

    /**
     * Tests the behavior of getting the broker configuration when it has not been initialized.
     * Expects an IllegalArgumentException to be thrown, ensuring that the configuration cannot be obtained without initialization.
     */
    @Test(expected = IllegalArgumentException.class)
    public void getBrokerConfig_NullConfiguration_ThrowsException() {
        BrokerConfigSingleton.getBrokerConfig();
    }

    /**
     * Tests the behavior of setting the broker configuration after it has already been initialized.
     * Expects an IllegalArgumentException to be thrown, ensuring that the configuration cannot be reset once set.
     * Also asserts that the returned brokerConfig instance is the same as the one set, confirming the singleton property.
     */
    @Test(expected = IllegalArgumentException.class)
    public void setBrokerConfig_AlreadyInitialized_ThrowsException() {
        BrokerConfig config = new BrokerConfig();
        BrokerConfigSingleton.setBrokerConfig(config);
        Assert.assertSame("Expected brokerConfig instance is not returned", config, BrokerConfigSingleton.getBrokerConfig());
        BrokerConfigSingleton.setBrokerConfig(config);
    }

}
