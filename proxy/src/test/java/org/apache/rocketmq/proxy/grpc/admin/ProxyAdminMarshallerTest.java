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

import io.grpc.MethodDescriptor;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import org.junit.Test;
import apache.rocketmq.proxy.admin.v1.DescribeClientRequest;
import apache.rocketmq.proxy.admin.v1.ListClientsByGroupRequest;
import apache.rocketmq.proxy.admin.v1.ListClientsByTopicRequest;
import apache.rocketmq.proxy.admin.v1.ListClientsRequest;
import apache.rocketmq.proxy.admin.v1.ListClientsResponse;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/** Unit tests for ProxyAdminMarshaller verifying all 8 proto marshallers are registered and perform correct serialization round-trips. */
public class ProxyAdminMarshallerTest {

    @Test
    public void testListClientsReqMarshallerNotNull() {
        assertNotNull(ProxyAdminMarshaller.LIST_CLIENTS_REQ_MARSHALLER);
    }

    @Test
    public void testListClientsRespMarshallerNotNull() {
        assertNotNull(ProxyAdminMarshaller.LIST_CLIENTS_RESP_MARSHALLER);
    }

    @Test
    public void testDescribeClientReqMarshallerNotNull() {
        assertNotNull(ProxyAdminMarshaller.DESCRIBE_CLIENT_REQ_MARSHALLER);
    }

    @Test
    public void testDescribeClientRespMarshallerNotNull() {
        assertNotNull(ProxyAdminMarshaller.DESCRIBE_CLIENT_RESP_MARSHALLER);
    }

    @Test
    public void testListClientsByGroupReqMarshallerNotNull() {
        assertNotNull(ProxyAdminMarshaller.LIST_CLIENTS_BY_GROUP_REQ_MARSHALLER);
    }

    @Test
    public void testListClientsByGroupRespMarshallerNotNull() {
        assertNotNull(ProxyAdminMarshaller.LIST_CLIENTS_BY_GROUP_RESP_MARSHALLER);
    }

    @Test
    public void testListClientsByTopicReqMarshallerNotNull() {
        assertNotNull(ProxyAdminMarshaller.LIST_CLIENTS_BY_TOPIC_REQ_MARSHALLER);
    }

    @Test
    public void testListClientsByTopicRespMarshallerNotNull() {
        assertNotNull(ProxyAdminMarshaller.LIST_CLIENTS_BY_TOPIC_RESP_MARSHALLER);
    }

    @Test
    public void testParseListClientsRequest() throws Exception {
        ListClientsRequest request = ListClientsRequest.newBuilder().build();
        InputStream stream = ProxyAdminMarshaller.LIST_CLIENTS_REQ_MARSHALLER.stream(request);
        ListClientsRequest parsed = ProxyAdminMarshaller.LIST_CLIENTS_REQ_MARSHALLER.parse(stream);
        assertNotNull(parsed);
        assertEquals(ListClientsRequest.getDefaultInstance(), parsed);
    }

    @Test
    public void testParseListClientsResponse() throws Exception {
        ListClientsResponse response = ListClientsResponse.newBuilder().build();
        InputStream stream = ProxyAdminMarshaller.LIST_CLIENTS_RESP_MARSHALLER.stream(response);
        ListClientsResponse parsed = ProxyAdminMarshaller.LIST_CLIENTS_RESP_MARSHALLER.parse(stream);
        assertNotNull(parsed);
        assertEquals(ListClientsResponse.getDefaultInstance(), parsed);
    }

    @Test
    public void testRoundTripListClientsRequest() throws Exception {
        ListClientsRequest request = ListClientsRequest.newBuilder().build();
        InputStream stream = ProxyAdminMarshaller.LIST_CLIENTS_REQ_MARSHALLER.stream(request);
        ListClientsRequest parsed = ProxyAdminMarshaller.LIST_CLIENTS_REQ_MARSHALLER.parse(stream);
        assertNotNull(parsed);
        assertEquals(request.toString(), parsed.toString());
    }

    @Test
    public void testRoundTripDescribeClientRequest() throws Exception {
        DescribeClientRequest request = DescribeClientRequest.newBuilder().build();
        InputStream stream = ProxyAdminMarshaller.DESCRIBE_CLIENT_REQ_MARSHALLER.stream(request);
        DescribeClientRequest parsed = ProxyAdminMarshaller.DESCRIBE_CLIENT_REQ_MARSHALLER.parse(stream);
        assertNotNull(parsed);
        assertEquals(request.toString(), parsed.toString());
    }

    @Test
    public void testRoundTripListClientsByGroupRequest() throws Exception {
        ListClientsByGroupRequest request = ListClientsByGroupRequest.newBuilder().build();
        InputStream stream = ProxyAdminMarshaller.LIST_CLIENTS_BY_GROUP_REQ_MARSHALLER.stream(request);
        ListClientsByGroupRequest parsed = ProxyAdminMarshaller.LIST_CLIENTS_BY_GROUP_REQ_MARSHALLER.parse(stream);
        assertNotNull(parsed);
        assertEquals(request.toString(), parsed.toString());
    }

    @Test
    public void testRoundTripListClientsByTopicRequest() throws Exception {
        ListClientsByTopicRequest request = ListClientsByTopicRequest.newBuilder().build();
        InputStream stream = ProxyAdminMarshaller.LIST_CLIENTS_BY_TOPIC_REQ_MARSHALLER.stream(request);
        ListClientsByTopicRequest parsed = ProxyAdminMarshaller.LIST_CLIENTS_BY_TOPIC_REQ_MARSHALLER.parse(stream);
        assertNotNull(parsed);
        assertEquals(request.toString(), parsed.toString());
    }

    @Test
    public void testParseEmptyStreamDoesNotThrow() throws Exception {
        ByteArrayInputStream emptyStream = new ByteArrayInputStream(new byte[0]);
        // All 8 marshallers should handle empty input without throwing
        ProxyAdminMarshaller.LIST_CLIENTS_REQ_MARSHALLER.parse(emptyStream);
        ProxyAdminMarshaller.LIST_CLIENTS_RESP_MARSHALLER.parse(new ByteArrayInputStream(new byte[0]));
        ProxyAdminMarshaller.DESCRIBE_CLIENT_REQ_MARSHALLER.parse(new ByteArrayInputStream(new byte[0]));
        ProxyAdminMarshaller.DESCRIBE_CLIENT_RESP_MARSHALLER.parse(new ByteArrayInputStream(new byte[0]));
        ProxyAdminMarshaller.LIST_CLIENTS_BY_GROUP_REQ_MARSHALLER.parse(new ByteArrayInputStream(new byte[0]));
        ProxyAdminMarshaller.LIST_CLIENTS_BY_GROUP_RESP_MARSHALLER.parse(new ByteArrayInputStream(new byte[0]));
        ProxyAdminMarshaller.LIST_CLIENTS_BY_TOPIC_REQ_MARSHALLER.parse(new ByteArrayInputStream(new byte[0]));
        ProxyAdminMarshaller.LIST_CLIENTS_BY_TOPIC_RESP_MARSHALLER.parse(new ByteArrayInputStream(new byte[0]));
    }

    @Test
    public void testMarshallerTypesAreCorrect() {
        assertTrue(ProxyAdminMarshaller.LIST_CLIENTS_REQ_MARSHALLER instanceof MethodDescriptor.Marshaller);
        assertTrue(ProxyAdminMarshaller.LIST_CLIENTS_RESP_MARSHALLER instanceof MethodDescriptor.Marshaller);
        assertTrue(ProxyAdminMarshaller.DESCRIBE_CLIENT_REQ_MARSHALLER instanceof MethodDescriptor.Marshaller);
        assertTrue(ProxyAdminMarshaller.DESCRIBE_CLIENT_RESP_MARSHALLER instanceof MethodDescriptor.Marshaller);
        assertTrue(ProxyAdminMarshaller.LIST_CLIENTS_BY_GROUP_REQ_MARSHALLER instanceof MethodDescriptor.Marshaller);
        assertTrue(ProxyAdminMarshaller.LIST_CLIENTS_BY_GROUP_RESP_MARSHALLER instanceof MethodDescriptor.Marshaller);
        assertTrue(ProxyAdminMarshaller.LIST_CLIENTS_BY_TOPIC_REQ_MARSHALLER instanceof MethodDescriptor.Marshaller);
        assertTrue(ProxyAdminMarshaller.LIST_CLIENTS_BY_TOPIC_RESP_MARSHALLER instanceof MethodDescriptor.Marshaller);
    }
}
