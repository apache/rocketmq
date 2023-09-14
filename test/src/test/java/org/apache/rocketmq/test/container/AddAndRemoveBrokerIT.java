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

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.container.BrokerContainer;
import org.apache.rocketmq.remoting.exception.RemotingConnectException;
import org.apache.rocketmq.remoting.exception.RemotingSendRequestException;
import org.apache.rocketmq.remoting.exception.RemotingTimeoutException;
import org.apache.rocketmq.remoting.protocol.ResponseCode;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

@Ignore
public class AddAndRemoveBrokerIT extends ContainerIntegrationTestBase {
    private static BrokerContainer brokerContainer4;

    @BeforeClass
    public static void beforeClass() {
        brokerContainer4 = createAndStartBrokerContainer(nsAddr);
    }

    @AfterClass
    public static void afterClass() {
        brokerContainer4.shutdown();
    }

    @Test
    public void addBrokerTest()
        throws InterruptedException, RemotingTimeoutException, RemotingSendRequestException,
        RemotingConnectException {
        String remark = null;
        int code = 0;
        try {
            defaultMQAdminExt.addBrokerToContainer(brokerContainer4.getBrokerContainerAddr(), "");
        } catch (MQBrokerException e) {
            code = e.getResponseCode();
            remark = e.getErrorMessage();
        }
        assertThat(code).isEqualTo(ResponseCode.SYSTEM_ERROR);
        assertThat(remark).isEqualTo("addBroker properties empty");
    }

    @Test
    public void removeBrokerTest()
        throws InterruptedException, RemotingTimeoutException, RemotingSendRequestException, RemotingConnectException {

        boolean exceptionCaught = false;

        try {
            defaultMQAdminExt.removeBrokerFromContainer(brokerContainer1.getBrokerContainerAddr(),
                master3With3Replicas.getBrokerConfig().getBrokerClusterName(),
                master3With3Replicas.getBrokerConfig().getBrokerName(), 1);
        } catch (MQBrokerException e) {
            exceptionCaught = true;
        }

        assertThat(exceptionCaught).isFalse();
        assertThat(brokerContainer1.getSlaveBrokers().size()).isEqualTo(1);

        createAndAddSlave(1, brokerContainer1, master3With3Replicas);
        awaitUntilSlaveOK();
    }
}
