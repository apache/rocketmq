/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.rocketmq.proxy.grpc.v2;

import apache.rocketmq.v2.Resource;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.topic.TopicValidator;
import org.apache.rocketmq.proxy.config.InitConfigTest;
import org.apache.rocketmq.proxy.grpc.v2.channel.GrpcChannelManager;
import org.apache.rocketmq.proxy.grpc.v2.common.GrpcClientSettingsManager;
import org.apache.rocketmq.proxy.grpc.v2.common.GrpcProxyException;
import org.apache.rocketmq.proxy.processor.MessagingProcessor;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertThrows;

public class AbstractMessingActivityTest extends InitConfigTest {

    public static class MockMessingActivity extends AbstractMessingActivity {

        public MockMessingActivity(MessagingProcessor messagingProcessor,
            GrpcClientSettingsManager grpcClientSettingsManager,
            GrpcChannelManager grpcChannelManager) {
            super(messagingProcessor, grpcClientSettingsManager, grpcChannelManager);
        }
    }

    private AbstractMessingActivity messingActivity;

    @Before
    public void before() throws Throwable {
        super.before();
        this.messingActivity = new MockMessingActivity(null, null, null);
    }

    @Test
    public void testValidateTopic() {
        assertThrows(GrpcProxyException.class, () -> messingActivity.validateTopic(Resource.newBuilder().build()));
        assertThrows(GrpcProxyException.class, () -> messingActivity.validateTopic(Resource.newBuilder().setName(TopicValidator.RMQ_SYS_TRACE_TOPIC).build()));
        assertThrows(GrpcProxyException.class, () -> messingActivity.validateTopic(Resource.newBuilder().setName("@").build()));
        assertThrows(GrpcProxyException.class, () -> messingActivity.validateTopic(Resource.newBuilder().setName(createString(128)).build()));
        messingActivity.validateTopic(Resource.newBuilder().setName(createString(127)).build());
    }

    @Test
    public void testValidateConsumer() {
        assertThrows(GrpcProxyException.class, () -> messingActivity.validateConsumerGroup(Resource.newBuilder().build()));
        assertThrows(GrpcProxyException.class, () -> messingActivity.validateConsumerGroup(Resource.newBuilder().setName(MixAll.CID_SYS_RMQ_TRANS).build()));
        assertThrows(GrpcProxyException.class, () -> messingActivity.validateConsumerGroup(Resource.newBuilder().setName("@").build()));
        assertThrows(GrpcProxyException.class, () -> messingActivity.validateConsumerGroup(Resource.newBuilder().setName(createString(256)).build()));
        messingActivity.validateConsumerGroup(Resource.newBuilder().setName(createString(120)).build());
    }

    private static String createString(int len) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < len; i++) {
            sb.append('a');
        }
        return sb.toString();
    }
}
