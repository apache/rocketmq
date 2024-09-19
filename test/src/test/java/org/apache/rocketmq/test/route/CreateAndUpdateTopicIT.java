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

package org.apache.rocketmq.test.route;

import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.remoting.protocol.route.TopicRouteData;
import org.apache.rocketmq.test.base.BaseConf;
import org.apache.rocketmq.test.util.MQAdminTestUtils;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

public class CreateAndUpdateTopicIT extends BaseConf {

    @Test
    public void testCreateOrUpdateTopic_EnableSingleTopicRegistration() {
        String topic = "test-topic-without-broker-registration";
        brokerController1.getBrokerConfig().setEnableSingleTopicRegister(true);
        brokerController2.getBrokerConfig().setEnableSingleTopicRegister(true);
        brokerController3.getBrokerConfig().setEnableSingleTopicRegister(true);

        final boolean createResult = MQAdminTestUtils.createTopic(NAMESRV_ADDR, CLUSTER_NAME, topic, 8, null);
        assertThat(createResult).isTrue();

        TopicRouteData route = MQAdminTestUtils.examineTopicRouteInfo(NAMESRV_ADDR, topic);
        assertThat(route.getBrokerDatas()).hasSize(3);
        assertThat(route.getQueueDatas()).hasSize(3);

        brokerController1.getBrokerConfig().setEnableSingleTopicRegister(false);
        brokerController2.getBrokerConfig().setEnableSingleTopicRegister(false);
        brokerController3.getBrokerConfig().setEnableSingleTopicRegister(false);

    }

    @Test
    public void testDeleteTopicFromNameSrvWithBrokerRegistration() {
        namesrvController.getNamesrvConfig().setDeleteTopicWithBrokerRegistration(true);
        brokerController1.getBrokerConfig().setEnableSingleTopicRegister(true);
        brokerController2.getBrokerConfig().setEnableSingleTopicRegister(true);
        brokerController3.getBrokerConfig().setEnableSingleTopicRegister(true);

        String testTopic1 = "test-topic-keep-route";
        String testTopic2 = "test-topic-delete-route";

        boolean createResult = MQAdminTestUtils.createTopic(NAMESRV_ADDR, CLUSTER_NAME, testTopic1, 8, null);
        assertThat(createResult).isTrue();

        createResult = MQAdminTestUtils.createTopic(NAMESRV_ADDR, CLUSTER_NAME, testTopic2, 8, null);
        assertThat(createResult).isTrue();

        TopicRouteData route = MQAdminTestUtils.examineTopicRouteInfo(NAMESRV_ADDR, testTopic2);
        assertThat(route.getBrokerDatas()).hasSize(3);

        MQAdminTestUtils.deleteTopicFromBrokerOnly(NAMESRV_ADDR, BROKER1_NAME, testTopic2);

        // Deletion is lazy, trigger broker registration
        brokerController1.registerBrokerAll(false, false, true);
        
        await().atMost(30, TimeUnit.SECONDS).until(() -> {
            // The route info of testTopic2 will be removed from broker1 after the registration
            TopicRouteData finalRoute = MQAdminTestUtils.examineTopicRouteInfo(NAMESRV_ADDR, testTopic2);
            return finalRoute.getBrokerDatas().size() == 2;
        });
        assertThat(route.getQueueDatas().get(0).getBrokerName()).isEqualTo(BROKER2_NAME);
        assertThat(route.getQueueDatas().get(1).getBrokerName()).isEqualTo(BROKER3_NAME);

        brokerController1.getBrokerConfig().setEnableSingleTopicRegister(false);
        brokerController2.getBrokerConfig().setEnableSingleTopicRegister(false);
        brokerController3.getBrokerConfig().setEnableSingleTopicRegister(false);
        namesrvController.getNamesrvConfig().setDeleteTopicWithBrokerRegistration(false);
    }

    @Test
    public void testStaticTopicNotAffected() throws Exception {
        namesrvController.getNamesrvConfig().setDeleteTopicWithBrokerRegistration(true);
        brokerController1.getBrokerConfig().setEnableSingleTopicRegister(true);
        brokerController2.getBrokerConfig().setEnableSingleTopicRegister(true);
        brokerController3.getBrokerConfig().setEnableSingleTopicRegister(true);

        String testTopic = "test-topic-not-affected";
        String testStaticTopic = "test-static-topic";

        boolean createResult = MQAdminTestUtils.createTopic(NAMESRV_ADDR, CLUSTER_NAME, testTopic, 8, null);
        assertThat(createResult).isTrue();

        TopicRouteData route = MQAdminTestUtils.examineTopicRouteInfo(NAMESRV_ADDR, testTopic);
        assertThat(route.getBrokerDatas()).hasSize(3);
        assertThat(route.getQueueDatas()).hasSize(3);

        MQAdminTestUtils.createStaticTopicWithCommand(testStaticTopic, 10, null, CLUSTER_NAME, NAMESRV_ADDR);

        assertThat(route.getBrokerDatas()).hasSize(3);
        assertThat(route.getQueueDatas()).hasSize(3);

        brokerController1.getBrokerConfig().setEnableSingleTopicRegister(false);
        brokerController2.getBrokerConfig().setEnableSingleTopicRegister(false);
        brokerController3.getBrokerConfig().setEnableSingleTopicRegister(false);
        namesrvController.getNamesrvConfig().setDeleteTopicWithBrokerRegistration(false);
    }

    @Test
    public void testCreateOrUpdateTopic_EnableSplitRegistration() {
        brokerController1.getBrokerConfig().setEnableSplitRegistration(true);
        brokerController2.getBrokerConfig().setEnableSplitRegistration(true);
        brokerController3.getBrokerConfig().setEnableSplitRegistration(true);

        String testTopic = "test-topic-";

        for (int i = 0; i < 10; i++) {
            TopicConfig topicConfig = new TopicConfig(testTopic + i, 8, 8);
            brokerController1.getTopicConfigManager().updateTopicConfig(topicConfig);
            brokerController2.getTopicConfigManager().updateTopicConfig(topicConfig);
            brokerController3.getTopicConfigManager().updateTopicConfig(topicConfig);
        }

        brokerController1.registerBrokerAll(false, true, true);
        brokerController2.registerBrokerAll(false, true, true);
        brokerController3.registerBrokerAll(false, true, true);

        for (int i = 0; i < 10; i++) {
            TopicRouteData route = MQAdminTestUtils.examineTopicRouteInfo(NAMESRV_ADDR, testTopic + i);
            assertThat(route.getBrokerDatas()).hasSize(3);
            assertThat(route.getQueueDatas()).hasSize(3);
        }

        brokerController1.getBrokerConfig().setEnableSplitRegistration(false);
        brokerController2.getBrokerConfig().setEnableSplitRegistration(false);
        brokerController3.getBrokerConfig().setEnableSplitRegistration(false);
    }
}
