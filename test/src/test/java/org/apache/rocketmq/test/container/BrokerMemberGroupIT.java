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

package org.apache.rocketmq.test.container;

import java.time.Duration;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.protocol.body.BrokerMemberGroup;
import org.junit.Ignore;
import org.junit.Test;

import static org.awaitility.Awaitility.await;

@Ignore
public class BrokerMemberGroupIT extends ContainerIntegrationTestBase {
    @Test
    public void testSyncBrokerMemberGroup() throws Exception {
        await().atMost(Duration.ofSeconds(5)).until(() -> {
            final BrokerConfig brokerConfig = master1With3Replicas.getBrokerConfig();
            final BrokerMemberGroup memberGroup = master1With3Replicas.getBrokerOuterAPI()
                .syncBrokerMemberGroup(brokerConfig.getBrokerClusterName(), brokerConfig.getBrokerName());

            return memberGroup.getBrokerAddrs().size() == 3;
        });

        await().atMost(Duration.ofSeconds(5)).until(() -> {
            final BrokerConfig brokerConfig = master3With3Replicas.getBrokerConfig();
            final BrokerMemberGroup memberGroup = master3With3Replicas.getBrokerOuterAPI()
                .syncBrokerMemberGroup(brokerConfig.getBrokerClusterName(), brokerConfig.getBrokerName());

            return memberGroup.getBrokerAddrs().size() == 3;
        });

        removeSlaveBroker(1, brokerContainer1, master3With3Replicas);
        removeSlaveBroker(1, brokerContainer2, master1With3Replicas);

        await().atMost(Duration.ofSeconds(5)).until(() -> {
            final BrokerConfig brokerConfig = master1With3Replicas.getBrokerConfig();
            final BrokerMemberGroup memberGroup = master1With3Replicas.getBrokerOuterAPI()
                .syncBrokerMemberGroup(brokerConfig.getBrokerClusterName(), brokerConfig.getBrokerName());

            return memberGroup.getBrokerAddrs().size() == 2 && memberGroup.getBrokerAddrs().get(1L) == null;
        });

        await().atMost(Duration.ofSeconds(5)).until(() -> {
            final BrokerConfig brokerConfig = master3With3Replicas.getBrokerConfig();
            final BrokerMemberGroup memberGroup = master3With3Replicas.getBrokerOuterAPI()
                .syncBrokerMemberGroup(brokerConfig.getBrokerClusterName(), brokerConfig.getBrokerName());
            return memberGroup.getBrokerAddrs().size() == 2 && memberGroup.getBrokerAddrs().get(1L) == null;
        });

        createAndAddSlave(1, brokerContainer2, master1With3Replicas);
        createAndAddSlave(1, brokerContainer1, master3With3Replicas);

        awaitUntilSlaveOK();
    }
}
