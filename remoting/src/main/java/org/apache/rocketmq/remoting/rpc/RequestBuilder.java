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

import java.util.HashMap;
import java.util.Map;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.protocol.RequestCode;
import org.apache.rocketmq.remoting.protocol.header.PullMessageRequestHeader;

public class RequestBuilder {

    private static Map<Integer, Class> requestCodeMap = new HashMap<>();
    static {
        requestCodeMap.put(RequestCode.PULL_MESSAGE, PullMessageRequestHeader.class);
    }

    public static RpcRequestHeader buildCommonRpcHeader(int requestCode, String destBrokerName) {
        return buildCommonRpcHeader(requestCode, null, destBrokerName);
    }

    public static RpcRequestHeader buildCommonRpcHeader(int requestCode, Boolean oneway, String destBrokerName) {
        Class requestHeaderClass = requestCodeMap.get(requestCode);
        if (requestHeaderClass == null) {
            throw new UnsupportedOperationException("unknown " + requestCode);
        }
        try {
            RpcRequestHeader requestHeader = (RpcRequestHeader) requestHeaderClass.newInstance();
            requestHeader.setOway(oneway);
            requestHeader.setBname(destBrokerName);
            return requestHeader;
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }

    public static TopicQueueRequestHeader buildTopicQueueRequestHeader(int requestCode, MessageQueue mq) {
        return buildTopicQueueRequestHeader(requestCode, null, mq.getBrokerName(), mq.getTopic(), mq.getQueueId(), null);
    }

    public static TopicQueueRequestHeader buildTopicQueueRequestHeader(int requestCode, MessageQueue mq, Boolean logic) {
        return buildTopicQueueRequestHeader(requestCode, null, mq.getBrokerName(), mq.getTopic(), mq.getQueueId(), logic);
    }

    public static TopicQueueRequestHeader buildTopicQueueRequestHeader(int requestCode, Boolean oneway, MessageQueue mq, Boolean logic) {
        return buildTopicQueueRequestHeader(requestCode, oneway, mq.getBrokerName(), mq.getTopic(), mq.getQueueId(), logic);
    }

    public static TopicQueueRequestHeader buildTopicQueueRequestHeader(int requestCode,  Boolean oneway, String destBrokerName, String topic, int queueId, Boolean logic) {
        Class requestHeaderClass = requestCodeMap.get(requestCode);
        if (requestHeaderClass == null) {
            throw new UnsupportedOperationException("unknown " + requestCode);
        }
        try {
            TopicQueueRequestHeader requestHeader = (TopicQueueRequestHeader) requestHeaderClass.newInstance();
            requestHeader.setOway(oneway);
            requestHeader.setBname(destBrokerName);
            requestHeader.setTopic(topic);
            requestHeader.setQueueId(queueId);
            requestHeader.setLo(logic);
            return requestHeader;
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }
}
