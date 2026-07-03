/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * The License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.rocketmq.proxy.grpc.admin;

import io.grpc.MethodDescriptor;
import io.grpc.protobuf.lite.ProtoLiteUtils;
import apache.rocketmq.proxy.admin.v1.ListClientsRequest;
import apache.rocketmq.proxy.admin.v1.ListClientsResponse;
import apache.rocketmq.proxy.admin.v1.DescribeClientRequest;
import apache.rocketmq.proxy.admin.v1.DescribeClientResponse;
import apache.rocketmq.proxy.admin.v1.ListClientsByGroupRequest;
import apache.rocketmq.proxy.admin.v1.ListClientsByGroupResponse;
import apache.rocketmq.proxy.admin.v1.ListClientsByTopicRequest;
import apache.rocketmq.proxy.admin.v1.ListClientsByTopicResponse;

/**
 * Protobuf-based Marshallers for Proxy Admin gRPC requests and responses.
 * <p>
 * Uses protobuf binary serialization as the wire format for admin RPCs,
 * replacing the previous JSON-based marshalling. This is the standard
 * approach for gRPC services defined via .proto files.
 * <p>
 * The proto definitions are compiled by protobuf-maven-plugin from
 * src/main/proto/proxy_admin.proto into Java classes in the
 * apache.rocketmq.proxy.admin.v1 package.
 */
public class ProxyAdminMarshaller {

    private ProxyAdminMarshaller() {
        // Utility class
    }

    // Request marshallers
    public static final MethodDescriptor.Marshaller<ListClientsRequest>
        LIST_CLIENTS_REQ_MARSHALLER = ProtoLiteUtils.marshaller(ListClientsRequest.getDefaultInstance());

    public static final MethodDescriptor.Marshaller<DescribeClientRequest>
        DESCRIBE_CLIENT_REQ_MARSHALLER = ProtoLiteUtils.marshaller(DescribeClientRequest.getDefaultInstance());

    public static final MethodDescriptor.Marshaller<ListClientsByGroupRequest>
        LIST_CLIENTS_BY_GROUP_REQ_MARSHALLER = ProtoLiteUtils.marshaller(ListClientsByGroupRequest.getDefaultInstance());

    public static final MethodDescriptor.Marshaller<ListClientsByTopicRequest>
        LIST_CLIENTS_BY_TOPIC_REQ_MARSHALLER = ProtoLiteUtils.marshaller(ListClientsByTopicRequest.getDefaultInstance());

    // Response marshallers
    public static final MethodDescriptor.Marshaller<ListClientsResponse>
        LIST_CLIENTS_RESP_MARSHALLER = ProtoLiteUtils.marshaller(ListClientsResponse.getDefaultInstance());

    public static final MethodDescriptor.Marshaller<DescribeClientResponse>
        DESCRIBE_CLIENT_RESP_MARSHALLER = ProtoLiteUtils.marshaller(DescribeClientResponse.getDefaultInstance());

    public static final MethodDescriptor.Marshaller<ListClientsByGroupResponse>
        LIST_CLIENTS_BY_GROUP_RESP_MARSHALLER = ProtoLiteUtils.marshaller(ListClientsByGroupResponse.getDefaultInstance());

    public static final MethodDescriptor.Marshaller<ListClientsByTopicResponse>
        LIST_CLIENTS_BY_TOPIC_RESP_MARSHALLER = ProtoLiteUtils.marshaller(ListClientsByTopicResponse.getDefaultInstance());
}