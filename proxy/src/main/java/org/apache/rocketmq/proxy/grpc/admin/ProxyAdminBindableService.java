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

import io.grpc.BindableService;
import io.grpc.MethodDescriptor;
import io.grpc.ServerServiceDefinition;
import io.grpc.ServiceDescriptor;
import io.grpc.stub.ServerCalls;
import io.grpc.stub.StreamObserver;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import apache.rocketmq.proxy.admin.v1.ListClientsRequest;
import apache.rocketmq.proxy.admin.v1.ListClientsResponse;
import apache.rocketmq.proxy.admin.v1.DescribeClientRequest;
import apache.rocketmq.proxy.admin.v1.DescribeClientResponse;
import apache.rocketmq.proxy.admin.v1.ListClientsByGroupRequest;
import apache.rocketmq.proxy.admin.v1.ListClientsByGroupResponse;
import apache.rocketmq.proxy.admin.v1.ListClientsByTopicRequest;
import apache.rocketmq.proxy.admin.v1.ListClientsByTopicResponse;

/**
 * BindableService wrapper for ProxyAdminGrpcService.
 * <p>
 * Constructs the gRPC ServiceDescriptor and method handlers using
 * protobuf-generated types and marshallers, registering the admin
 * service with the gRPC server.
 * <p>
 * Service name: apache.rocketmq.proxy.v2.ProxyClientAdminService
 * Methods:
 * - ListClients (UNARY)
 * - DescribeClient (UNARY)
 * - ListClientsByGroup (UNARY)
 * - ListClientsByTopic (UNARY)
 */
public class ProxyAdminBindableService implements BindableService {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.PROXY_LOGGER_NAME);

    public static final String SERVICE_NAME = "apache.rocketmq.proxy.admin.v1.ProxyClientAdminService";

    /** Method descriptor for ListClients RPC, reusable by both server and client. */
    public static final MethodDescriptor<ListClientsRequest, ListClientsResponse>
        LIST_CLIENTS_METHOD = MethodDescriptor.<ListClientsRequest, ListClientsResponse>newBuilder()
        .setType(MethodDescriptor.MethodType.UNARY)
        .setFullMethodName(MethodDescriptor.generateFullMethodName(SERVICE_NAME, "ListClients"))
        .setRequestMarshaller(ProxyAdminMarshaller.LIST_CLIENTS_REQ_MARSHALLER)
        .setResponseMarshaller(ProxyAdminMarshaller.LIST_CLIENTS_RESP_MARSHALLER)
        .build();

    /** Method descriptor for DescribeClient RPC, reusable by both server and client. */
    public static final MethodDescriptor<DescribeClientRequest, DescribeClientResponse>
        DESCRIBE_CLIENT_METHOD = MethodDescriptor.<DescribeClientRequest, DescribeClientResponse>newBuilder()
        .setType(MethodDescriptor.MethodType.UNARY)
        .setFullMethodName(MethodDescriptor.generateFullMethodName(SERVICE_NAME, "DescribeClient"))
        .setRequestMarshaller(ProxyAdminMarshaller.DESCRIBE_CLIENT_REQ_MARSHALLER)
        .setResponseMarshaller(ProxyAdminMarshaller.DESCRIBE_CLIENT_RESP_MARSHALLER)
        .build();

    /** Method descriptor for ListClientsByGroup RPC, reusable by both server and client. */
    public static final MethodDescriptor<ListClientsByGroupRequest, ListClientsByGroupResponse>
        LIST_CLIENTS_BY_GROUP_METHOD = MethodDescriptor.<ListClientsByGroupRequest, ListClientsByGroupResponse>newBuilder()
        .setType(MethodDescriptor.MethodType.UNARY)
        .setFullMethodName(MethodDescriptor.generateFullMethodName(SERVICE_NAME, "ListClientsByGroup"))
        .setRequestMarshaller(ProxyAdminMarshaller.LIST_CLIENTS_BY_GROUP_REQ_MARSHALLER)
        .setResponseMarshaller(ProxyAdminMarshaller.LIST_CLIENTS_BY_GROUP_RESP_MARSHALLER)
        .build();

    /** Method descriptor for ListClientsByTopic RPC, reusable by both server and client. */
    public static final MethodDescriptor<ListClientsByTopicRequest, ListClientsByTopicResponse>
        LIST_CLIENTS_BY_TOPIC_METHOD = MethodDescriptor.<ListClientsByTopicRequest, ListClientsByTopicResponse>newBuilder()
        .setType(MethodDescriptor.MethodType.UNARY)
        .setFullMethodName(MethodDescriptor.generateFullMethodName(SERVICE_NAME, "ListClientsByTopic"))
        .setRequestMarshaller(ProxyAdminMarshaller.LIST_CLIENTS_BY_TOPIC_REQ_MARSHALLER)
        .setResponseMarshaller(ProxyAdminMarshaller.LIST_CLIENTS_BY_TOPIC_RESP_MARSHALLER)
        .build();

    /** Service descriptor for ProxyClientAdminService. */
    public static final ServiceDescriptor SERVICE_DESCRIPTOR = ServiceDescriptor.newBuilder(SERVICE_NAME)
        .addMethod(LIST_CLIENTS_METHOD)
        .addMethod(DESCRIBE_CLIENT_METHOD)
        .addMethod(LIST_CLIENTS_BY_GROUP_METHOD)
        .addMethod(LIST_CLIENTS_BY_TOPIC_METHOD)
        .build();

    private final ProxyAdminGrpcService delegate;

    public ProxyAdminBindableService(ProxyAdminGrpcService delegate) {
        this.delegate = delegate;
    }

    @Override
    public ServerServiceDefinition bindService() {
        // Build server service definition with handlers using the public static method descriptors
        return ServerServiceDefinition.builder(SERVICE_DESCRIPTOR)
            .addMethod(LIST_CLIENTS_METHOD, ServerCalls.asyncUnaryCall(
                (request, responseObserver) -> delegate.listClients(request, responseObserver)))
            .addMethod(DESCRIBE_CLIENT_METHOD, ServerCalls.asyncUnaryCall(
                (request, responseObserver) -> delegate.describeClient(request, responseObserver)))
            .addMethod(LIST_CLIENTS_BY_GROUP_METHOD, ServerCalls.asyncUnaryCall(
                (request, responseObserver) -> delegate.listClientsByGroup(request, responseObserver)))
            .addMethod(LIST_CLIENTS_BY_TOPIC_METHOD, ServerCalls.asyncUnaryCall(
                (request, responseObserver) -> delegate.listClientsByTopic(request, responseObserver)))
            .build();
    }
}