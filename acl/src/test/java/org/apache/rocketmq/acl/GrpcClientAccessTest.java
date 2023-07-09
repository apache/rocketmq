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
package org.apache.rocketmq.acl;

import apache.rocketmq.v2.Message;
import apache.rocketmq.v2.MessageQueue;
import apache.rocketmq.v2.ReceiveMessageRequest;
import apache.rocketmq.v2.Resource;
import apache.rocketmq.v2.SendMessageRequest;
import java.io.File;
import java.io.IOException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import org.apache.rocketmq.acl.common.AclException;
import org.apache.rocketmq.acl.common.AuthenticationHeader;
import org.apache.rocketmq.acl.plain.AclTestHelper;
import org.apache.rocketmq.acl.plain.PlainAccessResource;
import org.apache.rocketmq.acl.plain.PlainAccessValidator;
import org.apache.rocketmq.client.apis.ClientConfiguration;
import org.apache.rocketmq.client.apis.SessionCredentialsProvider;
import org.apache.rocketmq.client.apis.StaticSessionCredentialsProvider;
import org.apache.rocketmq.client.java.misc.ClientId;
import org.apache.rocketmq.client.java.rpc.Signature;
import org.apache.rocketmq.remoting.protocol.RequestCode;
import org.apache.rocketmq.shaded.io.grpc.Metadata;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class GrpcClientAccessTest {

    private PlainAccessValidator plainAccessValidator;

    private File confHome;

    AuthenticationHeader authenticationHeader;

    @Before
    public void init() throws IOException, NoSuchAlgorithmException, InvalidKeyException {
        String folder = "conf";
        confHome = AclTestHelper.copyResources(folder, true);
        System.setProperty("rocketmq.home.dir", confHome.getAbsolutePath());
        plainAccessValidator = new PlainAccessValidator();

        String accessKey = "rocketmq3";
        String secretKey = "12345678";
        String endpoint = "127.0.0.1:8081";
        String clientAddress = "10.7.1.3";
        SessionCredentialsProvider sessionCredentialsProvider =
            new StaticSessionCredentialsProvider(accessKey, secretKey);
        ClientConfiguration clientConfiguration = ClientConfiguration.newBuilder()
            .setCredentialProvider(sessionCredentialsProvider)
            .setEndpoints(endpoint)
            .build();
        Metadata metadata = Signature.sign(clientConfiguration, new ClientId());

        authenticationHeader = AuthenticationHeader.builder()
            .authorization(metadata.get(Metadata.Key.of(Signature.AUTHORIZATION_KEY, Metadata.ASCII_STRING_MARSHALLER)))
            .datetime(metadata.get(Metadata.Key.of(Signature.DATE_TIME_KEY, Metadata.ASCII_STRING_MARSHALLER)))
            .remoteAddress(clientAddress)
            .build();
    }

    @After
    public void cleanUp() {
        AclTestHelper.recursiveDelete(confHome);
    }

    @Test(expected = AclException.class)
    public void testProduceDenyTopic() {
        authenticationHeader.setRequestCode(RequestCode.SEND_MESSAGE_V2);

        PlainAccessResource accessResource = (PlainAccessResource) plainAccessValidator.parse(
            getMockSendMessageRequest("topicD"), authenticationHeader);
        plainAccessValidator.validate(accessResource);

    }

    @Test
    public void testProduceAuthorizedTopic() {
        authenticationHeader.setRequestCode(RequestCode.SEND_MESSAGE_V2);

        PlainAccessResource accessResource = (PlainAccessResource) plainAccessValidator.parse(
            getMockSendMessageRequest("topicA"), authenticationHeader);
        plainAccessValidator.validate(accessResource);
    }

    private SendMessageRequest getMockSendMessageRequest(String topic) {
        return SendMessageRequest.newBuilder()
            .addMessages(Message.newBuilder()
                .setTopic(Resource.newBuilder()
                    .setName(topic)))
            .build();
    }

    @Test(expected = AclException.class)
    public void testConsumeDenyTopic() {
        authenticationHeader.setRequestCode(RequestCode.PULL_MESSAGE);

        PlainAccessResource accessResource = (PlainAccessResource) plainAccessValidator.parse(
            getMockReceiveMessageRequest("topicD", "groupB"), authenticationHeader);
        plainAccessValidator.validate(accessResource);

    }

    @Test
    public void testConsumeAuthorizedTopic() {
        authenticationHeader.setRequestCode(RequestCode.PULL_MESSAGE);

        PlainAccessResource accessResource = (PlainAccessResource) plainAccessValidator.parse(
            getMockReceiveMessageRequest("topicB", "groupB"), authenticationHeader);
        plainAccessValidator.validate(accessResource);
    }

    @Test(expected = AclException.class)
    public void testConsumeInDeniedGroup() {
        authenticationHeader.setRequestCode(RequestCode.PULL_MESSAGE);

        PlainAccessResource accessResource = (PlainAccessResource) plainAccessValidator.parse(
            getMockReceiveMessageRequest("topicB", "groupD"), authenticationHeader);
        plainAccessValidator.validate(accessResource);
    }

    @Test
    public void testConsumeInAuthorizedGroup() {
        authenticationHeader.setRequestCode(RequestCode.PULL_MESSAGE);

        PlainAccessResource accessResource = (PlainAccessResource) plainAccessValidator.parse(
            getMockReceiveMessageRequest("topicB", "groupB"), authenticationHeader);
        plainAccessValidator.validate(accessResource);
    }

    ReceiveMessageRequest getMockReceiveMessageRequest(String topic, String group) {
        return ReceiveMessageRequest.newBuilder()
            .setGroup(Resource.newBuilder()
                .setName(group))
            .setMessageQueue(MessageQueue.newBuilder().setTopic(Resource.newBuilder()
                .setName(topic)))
            .build();
    }

}
