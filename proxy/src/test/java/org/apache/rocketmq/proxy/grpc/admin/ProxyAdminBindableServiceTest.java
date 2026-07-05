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
import io.grpc.ServerMethodDefinition;
import io.grpc.ServerServiceDefinition;
import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/** Unit tests for ProxyAdminBindableService verifying service descriptor creation, RPC method registration, and null-delegate safety. */
public class ProxyAdminBindableServiceTest {

    @Test
    public void testBindServiceReturnsNonNull() {
        ProxyAdminGrpcService mockDelegate = Mockito.mock(ProxyAdminGrpcService.class);
        ProxyAdminBindableService service = new ProxyAdminBindableService(mockDelegate);
        ServerServiceDefinition definition = service.bindService();
        assertNotNull(definition);
    }

    @Test
    public void testServiceName() {
        assertEquals("apache.rocketmq.proxy.admin.v1.ProxyClientAdminService",
            ProxyAdminBindableService.SERVICE_NAME);
    }

    @Test
    public void testServiceDescriptorNotNull() {
        assertNotNull(ProxyAdminBindableService.SERVICE_DESCRIPTOR);
    }

    @Test
    public void testServiceDescriptorHasFourMethods() {
        assertEquals(4, ProxyAdminBindableService.SERVICE_DESCRIPTOR.getMethods().size());
    }

    @Test
    public void testServiceDescriptorMethodNames() {
        java.util.Collection<io.grpc.MethodDescriptor<?, ?>> methods =
            ProxyAdminBindableService.SERVICE_DESCRIPTOR.getMethods();
        java.util.Set<String> methodNames = new java.util.HashSet<>();
        for (io.grpc.MethodDescriptor<?, ?> method : methods) {
            methodNames.add(method.getBareMethodName());
        }
        assertEquals(4, methodNames.size());
        assertTrue(methodNames.contains("ListClients"));
        assertTrue(methodNames.contains("DescribeClient"));
        assertTrue(methodNames.contains("ListClientsByGroup"));
        assertTrue(methodNames.contains("ListClientsByTopic"));
    }

    @Test
    public void testListClientsMethod() {
        assertNotNull(ProxyAdminBindableService.LIST_CLIENTS_METHOD);
        assertEquals(MethodDescriptor.MethodType.UNARY,
            ProxyAdminBindableService.LIST_CLIENTS_METHOD.getType());
    }

    @Test
    public void testDescribeClientMethod() {
        assertNotNull(ProxyAdminBindableService.DESCRIBE_CLIENT_METHOD);
        assertEquals(MethodDescriptor.MethodType.UNARY,
            ProxyAdminBindableService.DESCRIBE_CLIENT_METHOD.getType());
    }

    @Test
    public void testListClientsByGroupMethod() {
        assertNotNull(ProxyAdminBindableService.LIST_CLIENTS_BY_GROUP_METHOD);
        assertEquals(MethodDescriptor.MethodType.UNARY,
            ProxyAdminBindableService.LIST_CLIENTS_BY_GROUP_METHOD.getType());
    }

    @Test
    public void testListClientsByTopicMethod() {
        assertNotNull(ProxyAdminBindableService.LIST_CLIENTS_BY_TOPIC_METHOD);
        assertEquals(MethodDescriptor.MethodType.UNARY,
            ProxyAdminBindableService.LIST_CLIENTS_BY_TOPIC_METHOD.getType());
    }

    @Test
    public void testBindServiceContainsExpectedMethodNames() {
        ProxyAdminGrpcService mockDelegate = Mockito.mock(ProxyAdminGrpcService.class);
        ProxyAdminBindableService service = new ProxyAdminBindableService(mockDelegate);
        ServerServiceDefinition definition = service.bindService();

        java.util.Set<String> methodNames = new java.util.HashSet<>();
        for (ServerMethodDefinition<?, ?> serverMethod : definition.getMethods()) {
            methodNames.add(serverMethod.getMethodDescriptor().getBareMethodName());
        }
        assertEquals(4, methodNames.size());
        assertTrue(methodNames.contains("ListClients"));
        assertTrue(methodNames.contains("DescribeClient"));
        assertTrue(methodNames.contains("ListClientsByGroup"));
        assertTrue(methodNames.contains("ListClientsByTopic"));
    }

    @Test
    public void testConstructorWithNullDelegateDoesNotThrow() {
        ProxyAdminBindableService service = null;
        try {
            service = new ProxyAdminBindableService(null);
        } catch (NullPointerException e) {
            // If NPE is thrown, this assertion will fail with a message
            throw new AssertionError("Constructor threw NPE with null delegate", e);
        }
        assertNotNull("Service instance should be created even with null delegate", service);
    }
}
