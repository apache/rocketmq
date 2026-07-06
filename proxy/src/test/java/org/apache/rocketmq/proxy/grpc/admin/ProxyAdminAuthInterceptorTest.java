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

package org.apache.rocketmq.proxy.grpc.admin;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import io.grpc.Metadata;
import org.apache.rocketmq.common.action.Action;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

/**
 * Unit tests for ProxyAdminAuthInterceptor.
 * <p>
 * Focuses on the resolveAction method which implements RIP-2 §7.2:
 * read = list/describe/get
 * - DescribeClient → Action.GET (single resource read)
 * - ListClients / ListClientsByGroup / ListClientsByTopic → Action.LIST (collection read)
 * <p>
 * Uses sun.misc.Unsafe to create instance without constructor to avoid
 * Mockito compatibility issues with Java 21 class files.
 */
public class ProxyAdminAuthInterceptorTest {

    private ProxyAdminAuthInterceptor interceptor;

    @Before
    public void setUp() throws Exception {
        interceptor = createInstanceWithoutConstructor();
        assertNotNull("Failed to create ProxyAdminAuthInterceptor instance", interceptor);
    }

    @After
    public void tearDown() {
        interceptor = null;
    }

    // ==================== resolveAction Tests ====================

    @Test
    public void testResolveAction_DescribeClient() throws Exception {
        Action action = invokeResolveAction("DescribeClient");
        assertEquals(Action.GET, action);
    }

    @Test
    public void testResolveAction_ListClients() throws Exception {
        Action action = invokeResolveAction("ListClients");
        assertEquals(Action.LIST, action);
    }

    @Test
    public void testResolveAction_ListClientsByGroup() throws Exception {
        Action action = invokeResolveAction("ListClientsByGroup");
        assertEquals(Action.LIST, action);
    }

    @Test
    public void testResolveAction_ListClientsByTopic() throws Exception {
        Action action = invokeResolveAction("ListClientsByTopic");
        assertEquals(Action.LIST, action);
    }

    @Test
    public void testResolveAction_FullMethodName_DescribeClient() throws Exception {
        // Test with full gRPC method name format
        Action action = invokeResolveAction("org.apache.rocketmq.proxy.admin/DescribeClient");
        assertEquals(Action.GET, action);
    }

    @Test
    public void testResolveAction_FullMethodName_ListClients() throws Exception {
        Action action = invokeResolveAction("org.apache.rocketmq.proxy.admin/ListClients");
        assertEquals(Action.LIST, action);
    }

    @Test
    public void testResolveAction_NullMethodName() throws Exception {
        Action action = invokeResolveAction(null);
        assertEquals(Action.LIST, action);
    }

    @Test
    public void testResolveAction_EmptyMethodName() throws Exception {
        Action action = invokeResolveAction("");
        assertEquals(Action.LIST, action);
    }

    @Test
    public void testResolveAction_UnknownMethod() throws Exception {
        Action action = invokeResolveAction("UnknownMethod");
        assertEquals(Action.LIST, action);
    }

    // ==================== Resource Constants Tests ====================

    @Test
    public void testResourceConstants() {
        assertEquals("proxy.admin", ProxyAdminAuthInterceptor.ADMIN_RESOURCE_PREFIX);
        assertEquals("proxy.admin.client", ProxyAdminAuthInterceptor.CLIENT_ADMIN_RESOURCE);
    }

    // ==================== extractUsername Tests ====================

    @Test
    public void testExtractUsername_WithAuthAk() throws Exception {
        Metadata headers = new Metadata();
        headers.put(org.apache.rocketmq.common.constant.GrpcConstants.AUTHORIZATION_AK, "testuser");
        String username = invokeExtractUsername(headers);
        assertEquals("testuser", username);
    }

    @Test
    public void testExtractUsername_BlankAuthAk() throws Exception {
        Metadata headers = new Metadata();
        headers.put(org.apache.rocketmq.common.constant.GrpcConstants.AUTHORIZATION_AK, "");
        String username = invokeExtractUsername(headers);
        assertEquals("unknown", username);
    }

    @Test
    public void testExtractUsername_NullHeaders() throws Exception {
        String username = invokeExtractUsername(null);
        assertEquals("unknown", username);
    }

    @Test
    public void testExtractUsername_NoAuthAk() throws Exception {
        Metadata headers = new Metadata();
        String username = invokeExtractUsername(headers);
        assertEquals("unknown", username);
    }

    // ==================== extractSourceIp Tests ====================

    @Test
    public void testExtractSourceIp_WithXForwardedFor() throws Exception {
        Metadata headers = new Metadata();
        headers.put(Metadata.Key.of("x-forwarded-for", Metadata.ASCII_STRING_MARSHALLER), "192.168.1.1");
        String ip = invokeExtractSourceIp(headers);
        assertEquals("192.168.1.1", ip);
    }

    @Test
    public void testExtractSourceIp_NullHeaders() throws Exception {
        String ip = invokeExtractSourceIp(null);
        assertNull(ip);
    }

    @Test
    public void testExtractSourceIp_BlankXForwardedFor() throws Exception {
        Metadata headers = new Metadata();
        headers.put(Metadata.Key.of("x-forwarded-for", Metadata.ASCII_STRING_MARSHALLER), "");
        String ip = invokeExtractSourceIp(headers);
        assertNull(ip);
    }

    @Test
    public void testExtractSourceIp_NoHeader() throws Exception {
        Metadata headers = new Metadata();
        String ip = invokeExtractSourceIp(headers);
        assertNull(ip);
    }

    // ==================== More resolveAction Tests ====================

    @Test
    public void testResolveAction_DescribeClientInPath() throws Exception {
        // DescribeClient appears anywhere in the path
        Action action = invokeResolveAction("apache.rocketmq.proxy.admin.v1.ProxyClientAdminService/DescribeClient");
        assertEquals(Action.GET, action);
    }

    @Test
    public void testResolveAction_ListClientsInPath() throws Exception {
        Action action = invokeResolveAction("apache.rocketmq.proxy.admin.v1.ProxyClientAdminService/ListClients");
        assertEquals(Action.LIST, action);
    }

    @Test
    public void testResolveAction_ListClientsByGroupInPath() throws Exception {
        Action action = invokeResolveAction("apache.rocketmq.proxy.admin.v1.ProxyClientAdminService/ListClientsByGroup");
        assertEquals(Action.LIST, action);
    }

    @Test
    public void testResolveAction_ListClientsByTopicInPath() throws Exception {
        Action action = invokeResolveAction("apache.rocketmq.proxy.admin.v1.ProxyClientAdminService/ListClientsByTopic");
        assertEquals(Action.LIST, action);
    }

    // ==================== Reflection Helpers ====================
    private Action invokeResolveAction(String methodName) throws Exception {
        Method method = ProxyAdminAuthInterceptor.class.getDeclaredMethod("resolveAction", String.class);
        method.setAccessible(true);
        return (Action) method.invoke(interceptor, methodName);
    }

    private String invokeExtractUsername(Metadata headers) throws Exception {
        Method method = ProxyAdminAuthInterceptor.class.getDeclaredMethod("extractUsername", Metadata.class);
        method.setAccessible(true);
        return (String) method.invoke(interceptor, new Object[]{headers});
    }

    private String invokeExtractSourceIp(Metadata headers) throws Exception {
        Method method = ProxyAdminAuthInterceptor.class.getDeclaredMethod("extractSourceIp", Metadata.class);
        method.setAccessible(true);
        return (String) method.invoke(interceptor, new Object[]{headers});
    }

    /**
     * Create a ProxyAdminAuthInterceptor instance without calling the constructor.
     * This avoids Mockito compatibility issues with Java 21 class files
     * and the complex AuthConfig/MessagingProcessor dependency chain.
     * Since resolveAction is a pure function, it doesn't need initialized fields.
     */
    @SuppressWarnings("unchecked")
    private static <T> T createInstanceWithoutConstructor() throws Exception {
        Class<?> unsafeClass = Class.forName("sun.misc.Unsafe");
        Field unsafeField = unsafeClass.getDeclaredField("theUnsafe");
        unsafeField.setAccessible(true);
        Object unsafe = unsafeField.get(null);
        Method allocateInstance = unsafeClass.getMethod("allocateInstance", Class.class);
        return (T) allocateInstance.invoke(unsafe, ProxyAdminAuthInterceptor.class);
    }
}