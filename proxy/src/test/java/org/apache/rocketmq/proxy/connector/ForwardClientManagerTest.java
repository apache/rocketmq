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

package org.apache.rocketmq.proxy.connector;

import org.apache.rocketmq.proxy.connector.transaction.TransactionStateChecker;
import org.apache.rocketmq.proxy.config.ConfigurationManager;
import org.apache.rocketmq.proxy.config.InitConfigAndLoggerTest;
import org.junit.Test;
import org.mockito.Mockito;

import static org.assertj.core.api.Assertions.assertThat;

public class ForwardClientManagerTest extends InitConfigAndLoggerTest {

    @Test
    public void testConnectorManager() throws Exception {
        TransactionStateChecker mockedTransactionStateChecker = Mockito.mock(TransactionStateChecker.class);
        ConnectorManager connectorManager = new ConnectorManager(mockedTransactionStateChecker);
        connectorManager.start();

        assertThat(connectorManager.getDefaultForwardClient()).isNotNull();
        assertThat(connectorManager.getDefaultForwardClient().getClientNum())
            .isEqualTo(ConfigurationManager.getProxyConfig().getDefaultForwardClientNum());

        assertThat(connectorManager.getForwardProducer()).isNotNull();
        assertThat(connectorManager.getForwardProducer().getClientNum())
            .isEqualTo(ConfigurationManager.getProxyConfig().getForwardProducerNum());

        assertThat(connectorManager.getForwardReadConsumer()).isNotNull();
        assertThat(connectorManager.getForwardReadConsumer().getClientNum())
            .isEqualTo(ConfigurationManager.getProxyConfig().getForwardConsumerNum());

        assertThat(connectorManager.getForwardWriteConsumer()).isNotNull();
        assertThat(connectorManager.getForwardWriteConsumer().getClientNum())
            .isEqualTo(ConfigurationManager.getProxyConfig().getForwardConsumerNum());

    }
}
