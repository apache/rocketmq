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
package org.apache.rocketmq.proxy.grpc.service.cluster;

import apache.rocketmq.v2.QueryAssignmentRequest;
import io.grpc.Context;
import java.util.List;
import org.apache.rocketmq.proxy.connector.route.MessageQueueWrapper;
import org.apache.rocketmq.proxy.connector.route.SelectableMessageQueue;
import org.apache.rocketmq.proxy.connector.route.TopicRouteCache;
import org.apache.rocketmq.proxy.grpc.adapter.GrpcConverterV2;

public class DefaultAssignmentQueueSelector implements AssignmentQueueSelector {

    private final TopicRouteCache topicRouteCache;

    public DefaultAssignmentQueueSelector(TopicRouteCache topicRouteCache) {
        this.topicRouteCache = topicRouteCache;
    }

    @Override
    public List<SelectableMessageQueue> getAssignment(Context ctx, QueryAssignmentRequest request) throws Exception {
        String topicName = GrpcConverterV2.wrapResourceWithNamespace(request.getTopic());
        MessageQueueWrapper messageQueueWrapper = topicRouteCache.getMessageQueue(topicName);
        return messageQueueWrapper.getReadSelector().getBrokerActingQueues();
    }
}