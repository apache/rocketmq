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

package org.apache.rocketmq.container;

import com.google.common.base.Preconditions;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.store.config.MessageStoreConfig;

public class InnerSalveBrokerController extends InnerBrokerController {

    private final Lock lock = new ReentrantLock();

    public InnerSalveBrokerController(final BrokerContainer brokerContainer,
        final BrokerConfig brokerConfig,
        final MessageStoreConfig storeConfig) {
        super(brokerContainer, brokerConfig, storeConfig);
        // Check configs
        checkSlaveBrokerConfig();
    }

    private void checkSlaveBrokerConfig() {
        Preconditions.checkNotNull(brokerConfig.getBrokerClusterName());
        Preconditions.checkNotNull(brokerConfig.getBrokerName());
        Preconditions.checkArgument(brokerConfig.getBrokerId() != MixAll.MASTER_ID);
    }
}
