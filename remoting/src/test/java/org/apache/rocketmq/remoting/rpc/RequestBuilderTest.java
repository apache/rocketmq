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

package org.apache.rocketmq.remoting.rpc;

import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.protocol.RequestCode;
import org.apache.rocketmq.remoting.protocol.header.PullMessageRequestHeader;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class RequestBuilderTest {

    private static final int[] UNKNOWN_REQUEST_CODES = {999999, -1};

    @Test
    public void testBuildTopicQueueRequestHeaderWithMessageQueue() {
        MessageQueue messageQueue = new MessageQueue("topicA", "brokerA", 3);
        TopicQueueRequestHeader header = RequestBuilder.buildTopicQueueRequestHeader(RequestCode.PULL_MESSAGE, messageQueue);

        assertThat(header).isInstanceOf(PullMessageRequestHeader.class);
        assertThat(header.getBrokerName()).isEqualTo("brokerA");
        assertThat(header.getTopic()).isEqualTo("topicA");
        assertThat(header.getQueueId()).isEqualTo(3);
        assertThat(header.getOneway()).isNull();
        assertThat(header.getLo()).isNull();
    }

    @Test
    public void testBuildTopicQueueRequestHeaderWithMessageQueueAndLogic() {
        MessageQueue messageQueue = new MessageQueue("topicB", "brokerB", 1);
        TopicQueueRequestHeader header = RequestBuilder.buildTopicQueueRequestHeader(RequestCode.PULL_MESSAGE, messageQueue, Boolean.TRUE);

        assertThat(header).isInstanceOf(PullMessageRequestHeader.class);
        assertThat(header.getBrokerName()).isEqualTo("brokerB");
        assertThat(header.getTopic()).isEqualTo("topicB");
        assertThat(header.getQueueId()).isEqualTo(1);
        assertThat(header.getOneway()).isNull();
        assertThat(header.getLo()).isTrue();
    }

    @Test
    public void testBuildTopicQueueRequestHeaderWithMessageQueueOnewayAndLogic() {
        MessageQueue messageQueue = new MessageQueue("topicC", "brokerC", 5);
        TopicQueueRequestHeader header = RequestBuilder.buildTopicQueueRequestHeader(RequestCode.PULL_MESSAGE, Boolean.TRUE, messageQueue, Boolean.FALSE);

        assertThat(header).isInstanceOf(PullMessageRequestHeader.class);
        assertThat(header.getBrokerName()).isEqualTo("brokerC");
        assertThat(header.getTopic()).isEqualTo("topicC");
        assertThat(header.getQueueId()).isEqualTo(5);
        assertThat(header.getOneway()).isTrue();
        assertThat(header.getLo()).isFalse();
    }

    @Test
    public void testBuildTopicQueueRequestHeaderWithFullArguments() {
        TopicQueueRequestHeader header = RequestBuilder.buildTopicQueueRequestHeader(
            RequestCode.PULL_MESSAGE, Boolean.TRUE, "brokerD", "topicD", 7, Boolean.TRUE);

        assertThat(header).isInstanceOf(PullMessageRequestHeader.class);
        assertThat(header.getBrokerName()).isEqualTo("brokerD");
        assertThat(header.getTopic()).isEqualTo("topicD");
        assertThat(header.getQueueId()).isEqualTo(7);
        assertThat(header.getOneway()).isTrue();
        assertThat(header.getLo()).isTrue();
    }

    @Test
    public void testBuildTopicQueueRequestHeaderRejectsUnknownRequestCode() {
        for (int requestCode : UNKNOWN_REQUEST_CODES) {
            assertThatThrownBy(() -> RequestBuilder.buildTopicQueueRequestHeader(requestCode, null, "brokerE", "topicE", 0, null))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("unknown")
                .hasMessageContaining(String.valueOf(requestCode));
        }
    }

    @Test
    public void testBuildCommonRpcHeaderRejectsUnknownRequestCode() {
        for (int requestCode : UNKNOWN_REQUEST_CODES) {
            assertThatThrownBy(() -> RequestBuilder.buildCommonRpcHeader(requestCode, "brokerF"))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("unknown")
                .hasMessageContaining(String.valueOf(requestCode));
        }
    }

    @Test
    public void testBuildCommonRpcHeaderWithDefaults() {
        RpcRequestHeader header = RequestBuilder.buildCommonRpcHeader(RequestCode.PULL_MESSAGE, "brokerG");

        assertThat(header).isInstanceOf(PullMessageRequestHeader.class);
        assertThat(header.getBrokerName()).isEqualTo("brokerG");
        assertThat(header.getOneway()).isNull();
    }

    @Test
    public void testBuildCommonRpcHeaderWithOneway() {
        RpcRequestHeader header = RequestBuilder.buildCommonRpcHeader(RequestCode.PULL_MESSAGE, Boolean.TRUE, "brokerH");

        assertThat(header).isInstanceOf(PullMessageRequestHeader.class);
        assertThat(header.getBrokerName()).isEqualTo("brokerH");
        assertThat(header.getOneway()).isTrue();
    }
}
