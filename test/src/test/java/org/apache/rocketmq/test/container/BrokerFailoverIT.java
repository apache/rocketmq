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
import org.apache.rocketmq.container.InnerSalveBrokerController;
import org.apache.rocketmq.common.protocol.RequestCode;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.junit.Ignore;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@Ignore
public class BrokerFailoverIT extends ContainerIntegrationTestBase {

    @Test
    public void testBrokerFailoverWithoutCompatible() {
        changeCompatibleMode(false);
        awaitUntilSlaveOK();
        testBrokerFailover(false);
    }

    @Test
    public void testBrokerFailoverWithCompatible() {
        changeCompatibleMode(true);
        awaitUntilSlaveOK();
        testBrokerFailover(true);
    }

    private void testBrokerFailover(boolean compatibleMode) {
        await().atMost(Duration.ofSeconds(10)).until(() ->
            master1With3Replicas.getMessageStore().getAliveReplicaNumInGroup() == 3
                && master2With3Replicas.getMessageStore().getAliveReplicaNumInGroup() == 3
                && master3With3Replicas.getMessageStore().getAliveReplicaNumInGroup() == 3);

        InnerSalveBrokerController targetSlave = getSlaveFromContainerByName(brokerContainer2, master1With3Replicas.getBrokerConfig().getBrokerName());

        assertThat(targetSlave).isNotNull();

        brokerContainer1.registerClientRPCHook(new RPCHook() {
            @Override
            public void doBeforeRequest(String remoteAddr, RemotingCommand request) {
                if (request.getCode() == (compatibleMode ? RequestCode.QUERY_DATA_VERSION : RequestCode.BROKER_HEARTBEAT)) {
                    request.setCode(-1);
                }
            }

            @Override
            public void doAfterResponse(String remoteAddr, RemotingCommand request,
                RemotingCommand response) {

            }
        });

        InnerSalveBrokerController finalTargetSlave = targetSlave;
        await().atMost(Duration.ofSeconds(60)).until(() ->
            finalTargetSlave.getMessageStore().getAliveReplicaNumInGroup() == 2
                && master2With3Replicas.getMessageStore().getAliveReplicaNumInGroup() == 2
                && master3With3Replicas.getMessageStore().getAliveReplicaNumInGroup() == 2);

        brokerContainer1.clearClientRPCHook();

        await().atMost(Duration.ofSeconds(60)).until(() ->
            master1With3Replicas.getMessageStore().getAliveReplicaNumInGroup() == 3
                && master2With3Replicas.getMessageStore().getAliveReplicaNumInGroup() == 3
                && master3With3Replicas.getMessageStore().getAliveReplicaNumInGroup() == 3);
    }
}
