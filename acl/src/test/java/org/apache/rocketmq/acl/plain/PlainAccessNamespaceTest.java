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

package org.apache.rocketmq.acl.plain;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.rocketmq.acl.common.AclClientRPCHook;
import org.apache.rocketmq.acl.common.AclUtils;
import org.apache.rocketmq.acl.common.Permission;
import org.apache.rocketmq.acl.common.SessionCredentials;
import org.apache.rocketmq.common.AclConfig;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.NamespaceAndPerm;
import org.apache.rocketmq.common.OperationType;
import org.apache.rocketmq.common.PlainAccessConfig;
import org.apache.rocketmq.common.ResourceAndPerm;
import org.apache.rocketmq.common.ResourceType;
import org.apache.rocketmq.common.protocol.RequestCode;
import org.apache.rocketmq.common.protocol.header.ConsumerSendMsgBackRequestHeader;
import org.apache.rocketmq.common.protocol.header.GetConsumerListByGroupRequestHeader;
import org.apache.rocketmq.common.protocol.header.PullMessageRequestHeader;
import org.apache.rocketmq.common.protocol.header.QueryMessageRequestHeader;
import org.apache.rocketmq.common.protocol.header.SendMessageRequestHeader;
import org.apache.rocketmq.common.protocol.header.SendMessageRequestHeaderV2;
import org.apache.rocketmq.common.protocol.header.UnregisterClientRequestHeader;
import org.apache.rocketmq.common.protocol.header.UpdateConsumerOffsetRequestHeader;
import org.apache.rocketmq.common.protocol.heartbeat.ConsumerData;
import org.apache.rocketmq.common.protocol.heartbeat.HeartbeatData;
import org.apache.rocketmq.common.protocol.heartbeat.ProducerData;
import org.apache.rocketmq.common.protocol.heartbeat.SubscriptionData;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class PlainAccessNamespaceTest {
    private PlainAccessValidator plainAccessValidator;
    private AclClientRPCHook aclClient;
    private SessionCredentials sessionCredentials;

    @Before
    public void init() {
        System.setProperty("rocketmq.home.dir", Paths.get("src/test/resources/namespace").toString());
        plainAccessValidator = new PlainAccessValidator();
        sessionCredentials = new SessionCredentials();
        sessionCredentials.setAccessKey("RocketMQ");
        sessionCredentials.setSecretKey("12345678");
        sessionCredentials.setSecurityToken("87654321");
        aclClient = new AclClientRPCHook(sessionCredentials);
    }

    @Test
    public void oldVersionTest() {
        AclConfig aclConfig = plainAccessValidator.getAllAclConfig();
        List<PlainAccessConfig> plainAccessConfigs = aclConfig.getPlainAccessConfigs();
        for (PlainAccessConfig plainAccessConfig : plainAccessConfigs) {
            if (plainAccessConfig.getAccessKey().equals("RocketMQ")) {
                Assert.assertEquals(plainAccessConfig.getSecretKey(), "12345678");
                Assert.assertEquals(plainAccessConfig.getWhiteRemoteAddress(), "192.168.0.*");
                Assert.assertEquals(plainAccessConfig.isAdmin(), false);
                Assert.assertEquals(plainAccessConfig.getDefaultGroupPerm(), "SUB");
                Assert.assertEquals(plainAccessConfig.getDefaultTopicPerm(), "DENY");
                Assert.assertEquals(plainAccessConfig.getTopicPerms().size(), 3);
                Assert.assertEquals(plainAccessConfig.getGroupPerms().size(), 3);
            } else if (plainAccessConfig.getAccessKey().equals("rocketmq2")) {
                Assert.assertEquals(plainAccessConfig.getSecretKey(), "12345678");
                Assert.assertEquals(plainAccessConfig.getWhiteRemoteAddress(), "192.168.1.*");
                Assert.assertEquals(plainAccessConfig.isAdmin(), true);
            }
        }
    }

    @Test
    public void validateSendMessageTest() {
        SendMessageRequestHeader messageRequestHeader = new SendMessageRequestHeader();
        messageRequestHeader.setTopic("namespace1%topicB");
        RemotingCommand remotingCommand = RemotingCommand.createRequestCommand(RequestCode.SEND_MESSAGE, messageRequestHeader);
        aclClient.doBeforeRequest("", remotingCommand);
        ByteBuffer buf = remotingCommand.encodeHeader();
        buf.getInt();
        buf = ByteBuffer.allocate(buf.limit() - buf.position()).put(buf);
        buf.position(0);
        try {
            PlainAccessResource accessResource = (PlainAccessResource) plainAccessValidator.parse(RemotingCommand.decode(buf), "192.168.0.1");
            Map<String, Byte> resourcePermMap = accessResource.getResourcePermMap();
            Assert.assertEquals(resourcePermMap.size(), 1);
            if (resourcePermMap.containsKey("namespace1%topicB")) {
                Byte perm = resourcePermMap.get("namespace1%topicB");
                Assert.assertEquals(Permission.checkPermission(Permission.PUB, perm), true);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void validateSendMessageV2Test() {
        SendMessageRequestHeader messageRequestHeader = new SendMessageRequestHeader();
        messageRequestHeader.setTopic("namespace1%topicB");
        RemotingCommand remotingCommand = RemotingCommand.createRequestCommand(RequestCode.SEND_MESSAGE_V2, SendMessageRequestHeaderV2.createSendMessageRequestHeaderV2(messageRequestHeader));
        aclClient.doBeforeRequest("", remotingCommand);

        ByteBuffer buf = remotingCommand.encodeHeader();
        buf.getInt();
        buf = ByteBuffer.allocate(buf.limit() - buf.position()).put(buf);
        buf.position(0);
        try {
            PlainAccessResource accessResource = (PlainAccessResource) plainAccessValidator.parse(RemotingCommand.decode(buf), "192.168.0.1:9876");
            Map<String, Byte> resourcePermMap = accessResource.getResourcePermMap();
            Assert.assertEquals(resourcePermMap.size(), 1);
            if (resourcePermMap.containsKey("namespace1%topicB")) {
                Byte perm = resourcePermMap.get("namespace1%topicB");
                Assert.assertEquals(Permission.checkPermission(Permission.PUB, perm), true);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void validatePullMessageTest() {
        PullMessageRequestHeader pullMessageRequestHeader = new PullMessageRequestHeader();
        pullMessageRequestHeader.setTopic("namespace1%topicB");
        pullMessageRequestHeader.setConsumerGroup("namespace1%consumerGroupA");
        RemotingCommand remotingCommand = RemotingCommand.createRequestCommand(RequestCode.PULL_MESSAGE, pullMessageRequestHeader);
        aclClient.doBeforeRequest("", remotingCommand);
        ByteBuffer buf = remotingCommand.encodeHeader();
        buf.getInt();
        buf = ByteBuffer.allocate(buf.limit() - buf.position()).put(buf);
        buf.position(0);
        try {
            PlainAccessResource accessResource = (PlainAccessResource) plainAccessValidator.parse(RemotingCommand.decode(buf), "192.168.0.1:9876");
            Map<String, Byte> resourcePermMap = accessResource.getResourcePermMap();
            Assert.assertEquals(resourcePermMap.size(), 2);
            if (resourcePermMap.containsKey("namespace1%topicB")) {
                Byte perm = resourcePermMap.get("namespace1%topicB");
                Assert.assertEquals(Permission.checkPermission(Permission.SUB, perm), true);
            }
            if (resourcePermMap.containsKey("%RETRY%namespace1%consumerGroupA")) {
                Byte perm = resourcePermMap.get("%RETRY%namespace1%consumerGroupA");
                Assert.assertEquals(Permission.checkPermission(Permission.SUB, perm), true);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void validateConsumeMessageBackTest() {
        ConsumerSendMsgBackRequestHeader consumerSendMsgBackRequestHeader = new ConsumerSendMsgBackRequestHeader();
        consumerSendMsgBackRequestHeader.setOriginTopic("namespace1%topicB");
        consumerSendMsgBackRequestHeader.setGroup("namespace1%consumerGroupA");
        RemotingCommand remotingCommand = RemotingCommand.createRequestCommand(RequestCode.CONSUMER_SEND_MSG_BACK, consumerSendMsgBackRequestHeader);
        aclClient.doBeforeRequest("", remotingCommand);
        ByteBuffer buf = remotingCommand.encodeHeader();
        buf.getInt();
        buf = ByteBuffer.allocate(buf.limit() - buf.position()).put(buf);
        buf.position(0);
        try {
            PlainAccessResource accessResource = (PlainAccessResource) plainAccessValidator.parse(RemotingCommand.decode(buf), "192.168.0.1:9876");
            Map<String, Byte> resourcePermMap = accessResource.getResourcePermMap();
            Assert.assertEquals(resourcePermMap.size(), 2);
            if (resourcePermMap.containsKey("namespace1%topicB")) {
                Byte perm = resourcePermMap.get("namespace1%topicB");
                Assert.assertEquals(Permission.checkPermission(Permission.PUB, perm), true);
            }
            if (resourcePermMap.containsKey("%RETRY%namespace1%consumerGroupA")) {
                Byte perm = resourcePermMap.get("%RETRY%namespace1%consumerGroupA");
                Assert.assertEquals(Permission.checkPermission(Permission.SUB, perm), true);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void validateQueryMessageTest() {
        QueryMessageRequestHeader queryMessageRequestHeader = new QueryMessageRequestHeader();
        queryMessageRequestHeader.setTopic("namespace1%topicB");
        RemotingCommand remotingCommand = RemotingCommand.createRequestCommand(RequestCode.QUERY_MESSAGE, queryMessageRequestHeader);
        aclClient.doBeforeRequest("", remotingCommand);
        ByteBuffer buf = remotingCommand.encodeHeader();
        buf.getInt();
        buf = ByteBuffer.allocate(buf.limit() - buf.position()).put(buf);
        buf.position(0);
        try {
            PlainAccessResource accessResource = (PlainAccessResource) plainAccessValidator.parse(RemotingCommand.decode(buf), "192.168.0.1:9876");
            Map<String, Byte> resourcePermMap = accessResource.getResourcePermMap();
            Assert.assertEquals(resourcePermMap.size(), 1);
            if (resourcePermMap.containsKey("namespace1%topicB")) {
                Byte perm = resourcePermMap.get("namespace1%topicB");
                Assert.assertEquals(Permission.checkPermission(Permission.SUB, perm), true);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void validateQueryMessageByKeyTest() {
        QueryMessageRequestHeader queryMessageRequestHeader = new QueryMessageRequestHeader();
        queryMessageRequestHeader.setTopic("namespace1%topicB");
        RemotingCommand remotingCommand = RemotingCommand.createRequestCommand(RequestCode.QUERY_MESSAGE, queryMessageRequestHeader);
        aclClient.doBeforeRequest("", remotingCommand);
        remotingCommand.addExtField(MixAll.UNIQUE_MSG_QUERY_FLAG, "false");
        ByteBuffer buf = remotingCommand.encodeHeader();
        buf.getInt();
        buf = ByteBuffer.allocate(buf.limit() - buf.position()).put(buf);
        buf.position(0);
        try {
            PlainAccessResource accessResource = (PlainAccessResource) plainAccessValidator.parse(RemotingCommand.decode(buf), "192.168.1.1:9876");
            Map<String, Byte> resourcePermMap = accessResource.getResourcePermMap();
            Assert.assertEquals(resourcePermMap.size(), 1);
            if (resourcePermMap.containsKey("namespace1%topicB")) {
                Byte perm = resourcePermMap.get("namespace1%topicB");
                Assert.assertEquals(Permission.checkPermission(Permission.SUB, perm), true);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void validateHeartBeatTest() {
        HeartbeatData heartbeatData = new HeartbeatData();
        Set<ProducerData> producerDataSet = new HashSet<>();
        Set<ConsumerData> consumerDataSet = new HashSet<>();
        Set<SubscriptionData> subscriptionDataSet = new HashSet<>();
        ProducerData producerData = new ProducerData();
        producerData.setGroupName("producerGroupA");
        ConsumerData consumerData = new ConsumerData();
        consumerData.setGroupName("namespace1%consumerGroupA");
        SubscriptionData subscriptionData = new SubscriptionData();
        subscriptionData.setTopic("namespace1%topicC");
        producerDataSet.add(producerData);
        consumerDataSet.add(consumerData);
        subscriptionDataSet.add(subscriptionData);
        consumerData.setSubscriptionDataSet(subscriptionDataSet);
        heartbeatData.setProducerDataSet(producerDataSet);
        heartbeatData.setConsumerDataSet(consumerDataSet);
        RemotingCommand remotingCommand = RemotingCommand.createRequestCommand(RequestCode.HEART_BEAT, null);
        remotingCommand.setBody(heartbeatData.encode());
        aclClient.doBeforeRequest("", remotingCommand);
        ByteBuffer buf = remotingCommand.encode();
        buf.getInt();
        buf = ByteBuffer.allocate(buf.limit() - buf.position()).put(buf);
        buf.position(0);
        try {
            PlainAccessResource accessResource = (PlainAccessResource) plainAccessValidator.parse(RemotingCommand.decode(buf), "192.168.0.1:9876");
            Map<String, Byte> resourcePermMap = accessResource.getResourcePermMap();
            Assert.assertEquals(resourcePermMap.size(), 2);
            if (resourcePermMap.containsKey("namespace1%topicC")) {
                Byte perm = resourcePermMap.get("namespace1%topicC");
                Assert.assertEquals(Permission.checkPermission(Permission.SUB, perm), true);
            }
            if (resourcePermMap.containsKey("%RETRY%namespace1%consumerGroupA")) {
                Byte perm = resourcePermMap.get("%RETRY%namespace1%consumerGroupA");
                Assert.assertEquals(Permission.checkPermission(Permission.SUB, perm), true);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void validateUnRegisterClientTest() {
        UnregisterClientRequestHeader unregisterClientRequestHeader = new UnregisterClientRequestHeader();
        unregisterClientRequestHeader.setConsumerGroup("namespace1%consumerGroupA");
        RemotingCommand remotingCommand = RemotingCommand.createRequestCommand(RequestCode.UNREGISTER_CLIENT, unregisterClientRequestHeader);
        aclClient.doBeforeRequest("", remotingCommand);
        ByteBuffer buf = remotingCommand.encodeHeader();
        buf.getInt();
        buf = ByteBuffer.allocate(buf.limit() - buf.position()).put(buf);
        buf.position(0);
        try {
            PlainAccessResource accessResource = (PlainAccessResource) plainAccessValidator.parse(RemotingCommand.decode(buf), "192.168.0.1:9876");
            Map<String, Byte> resourcePermMap = accessResource.getResourcePermMap();
            Assert.assertEquals(resourcePermMap.size(), 1);
            if (resourcePermMap.containsKey("%RETRY%namespace1%consumerGroupA")) {
                Byte perm = resourcePermMap.get("%RETRY%namespace1%consumerGroupA");
                Assert.assertEquals(Permission.checkPermission(Permission.SUB, perm), true);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void validateGetConsumerListByGroupTest() {
        GetConsumerListByGroupRequestHeader getConsumerListByGroupRequestHeader = new GetConsumerListByGroupRequestHeader();
        getConsumerListByGroupRequestHeader.setConsumerGroup("namespace1%consumerGroupA");
        RemotingCommand remotingCommand = RemotingCommand.createRequestCommand(RequestCode.GET_CONSUMER_LIST_BY_GROUP, getConsumerListByGroupRequestHeader);
        aclClient.doBeforeRequest("", remotingCommand);
        ByteBuffer buf = remotingCommand.encodeHeader();
        buf.getInt();
        buf = ByteBuffer.allocate(buf.limit() - buf.position()).put(buf);
        buf.position(0);
        try {
            PlainAccessResource accessResource = (PlainAccessResource) plainAccessValidator.parse(RemotingCommand.decode(buf), "192.168.0.1:9876");
            Map<String, Byte> resourcePermMap = accessResource.getResourcePermMap();
            Assert.assertEquals(resourcePermMap.size(), 1);
            if (resourcePermMap.containsKey("%RETRY%namespace1%consumerGroupA")) {
                Byte perm = resourcePermMap.get("%RETRY%namespace1%consumerGroupA");
                Assert.assertEquals(Permission.checkPermission(Permission.SUB, perm), true);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void validateUpdateConsumerOffSetTest() {
        UpdateConsumerOffsetRequestHeader updateConsumerOffsetRequestHeader = new UpdateConsumerOffsetRequestHeader();
        updateConsumerOffsetRequestHeader.setConsumerGroup("namespace1%consumerGroupA");
        RemotingCommand remotingCommand = RemotingCommand.createRequestCommand(RequestCode.UPDATE_CONSUMER_OFFSET, updateConsumerOffsetRequestHeader);
        aclClient.doBeforeRequest("", remotingCommand);
        ByteBuffer buf = remotingCommand.encodeHeader();
        buf.getInt();
        buf = ByteBuffer.allocate(buf.limit() - buf.position()).put(buf);
        buf.position(0);
        try {
            PlainAccessResource accessResource = (PlainAccessResource) plainAccessValidator.parse(RemotingCommand.decode(buf), "192.168.0.1:9876");
            Map<String, Byte> resourcePermMap = accessResource.getResourcePermMap();
            Assert.assertEquals(resourcePermMap.size(), 1);
            if (resourcePermMap.containsKey("%RETRY%namespace1%consumerGroupA")) {
                Byte perm = resourcePermMap.get("%RETRY%namespace1%consumerGroupA");
                Assert.assertEquals(Permission.checkPermission(Permission.SUB, perm), true);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void updateAclAccountTest() throws IOException, InterruptedException {
        String fileName = System.getProperty("rocketmq.home.dir") + File.separator + "conf/acl/plain_acl_test.yml";
        String defaultFileName = System.getProperty("rocketmq.home.dir") + File.separator + "conf/plain_acl.yml";
        Map<String, Object> backUpAclConfigMap = AclUtils.getYamlDataObject(defaultFileName, Map.class);

        File transport = new File(fileName);
        if (transport.exists()) {
            transport.delete();
        }
        transport.createNewFile();
        FileWriter writer = new FileWriter(transport);
        writer.write("accounts:\r\n");
        writer.write("- accessKey: rocketmqx\r\n");
        writer.write("  secretKey: 12345678\r\n");
        writer.write("  whiteRemoteAddress: 127.0.0.1\r\n");
        writer.write("  admin: false\r\n");
        writer.write("  resourcePerms:\r\n");
        writer.write("  - resource: topicA\r\n");
        writer.write("    type: TOPIC\r\n");
        writer.write("    namespace: namespace1\r\n");
        writer.write("    perm: PUB\r\n");
        writer.write("  namespacePerms:\r\n");
        writer.write("  - namespace: namespace1\r\n");
        writer.write("    topicPerm: DENY\r\n");
        writer.write("    groupPerm: DENY\r\n");
        writer.flush();
        writer.close();

        Thread.sleep(1000);

        PlainAccessValidator plainAccessValidator = new PlainAccessValidator();
        AclConfig aclConfig = plainAccessValidator.getAllAclConfig();
        List<PlainAccessConfig> plainAccessConfigs = aclConfig.getPlainAccessConfigs();
        for (PlainAccessConfig plainAccessConfig : plainAccessConfigs) {
            if (plainAccessConfig.getAccessKey().equals("rocketmqx")) {
                Assert.assertEquals(plainAccessConfig.getResourcePerms().get(0).getResource(), "topicA");
                Assert.assertEquals(plainAccessConfig.getResourcePerms().get(0).getType(), ResourceType.TOPIC);
                Assert.assertEquals(plainAccessConfig.getResourcePerms().get(0).getNamespace(), "namespace1");
                Assert.assertEquals(plainAccessConfig.getResourcePerms().get(0).getPerm(), "PUB");
                Assert.assertEquals(plainAccessConfig.getNamespacePerms().get(0).getNamespace(), "namespace1");
                Assert.assertEquals(plainAccessConfig.getNamespacePerms().get(0).getTopicPerm(), "DENY");
                Assert.assertEquals(plainAccessConfig.getNamespacePerms().get(0).getGroupPerm(), "DENY");
                break;
            }
        }

        PlainAccessConfig plainAccessConfig = new PlainAccessConfig();
        plainAccessConfig.setAccessKey("rocketmqy");
        plainAccessConfig.setSecretKey("12345678");
        plainAccessConfig.setAdmin(false);
        plainAccessConfig.setWhiteRemoteAddress("127.0.0.1");
        plainAccessConfig.setDefaultTopicPerm("DENY");
        plainAccessConfig.setDefaultGroupPerm("DENY");
        plainAccessValidator.updateAclAccount(plainAccessConfig);
        for (PlainAccessConfig plainAccessConfig1 : plainAccessValidator.getAllAclConfig().getPlainAccessConfigs()) {
            if (plainAccessConfig1.getAccessKey().equals("rocketmqy")) {
                Assert.assertEquals(plainAccessConfig.getSecretKey(), "12345678");
                Assert.assertEquals(plainAccessConfig.isAdmin(), false);
                Assert.assertEquals(plainAccessConfig.getWhiteRemoteAddress(), "127.0.0.1");
                Assert.assertEquals(plainAccessConfig.getDefaultTopicPerm(), "DENY");
                Assert.assertEquals(plainAccessConfig.getDefaultGroupPerm(), "DENY");
                break;
            }
        }

        transport.delete();
        File bak = new File(fileName + ".bak");
        if (bak.exists()) {
            bak.delete();
        }
        AclUtils.writeDataObject(defaultFileName, backUpAclConfigMap);
        File defaultFileBak = new File(defaultFileName + ".bak");
        if (defaultFileBak.exists()) {
            defaultFileBak.delete();
        }
    }

    @Test
    public void updateAclResourcePermsTest() throws IOException, InterruptedException {
        String fileName = System.getProperty("rocketmq.home.dir") + File.separator + "conf/acl/plain_acl_test.yml";
        File transport = new File(fileName);
        if (transport.exists()) {
            transport.delete();
        }
        transport.createNewFile();
        FileWriter writer = new FileWriter(transport);
        writer.write("accounts:\r\n");
        writer.write("- accessKey: rocketmqx\r\n");
        writer.write("  secretKey: 12345678\r\n");
        writer.write("  whiteRemoteAddress: 127.0.0.1\r\n");
        writer.write("  admin: false\r\n");
        writer.write("  resourcePerms:\r\n");
        writer.write("  - resource: topicA\r\n");
        writer.write("    type: TOPIC\r\n");
        writer.write("    namespace: namespace1\r\n");
        writer.write("    perm: PUB\r\n");
        writer.write("  namespacePerms:\r\n");
        writer.write("  - namespace: namespace1\r\n");
        writer.write("    topicPerm: DENY\r\n");
        writer.write("    groupPerm: DENY\r\n");
        writer.flush();
        writer.close();

        Thread.sleep(1000);

        String accesskey = "rocketmqx";
        ResourceAndPerm resourceAndPerm = new ResourceAndPerm();
        resourceAndPerm.setResource("topicB");
        resourceAndPerm.setType(ResourceType.TOPIC);
        resourceAndPerm.setNamespace("namespace1");
        resourceAndPerm.setPerm("PUB");
        PlainAccessValidator plainAccessValidator = new PlainAccessValidator();
        plainAccessValidator.updateAclResourcePerms(accesskey, resourceAndPerm, OperationType.ADD);
        for (PlainAccessConfig plainAccessConfig : plainAccessValidator.getAllAclConfig().getPlainAccessConfigs()) {
            if (plainAccessConfig.getAccessKey().equals("rocketmqx")) {
                List<ResourceAndPerm> resourceAndPerms = plainAccessConfig.getResourcePerms();
                for (ResourceAndPerm resourceAndPerm1 : resourceAndPerms) {
                    if (resourceAndPerm1.getResource().equals("topicB")) {
                        Assert.assertEquals(resourceAndPerm1.getType(), ResourceType.TOPIC);
                        Assert.assertEquals(resourceAndPerm1.getNamespace(), "namespace1");
                        Assert.assertEquals(resourceAndPerm1.getPerm(), "PUB");
                        break;
                    }
                }
                break;
            }
        }
        resourceAndPerm.setPerm("PUB|SUB");
        plainAccessValidator.updateAclResourcePerms(accesskey, resourceAndPerm, OperationType.UPDATE);
        Thread.sleep(1000);
        for (PlainAccessConfig plainAccessConfig : plainAccessValidator.getAllAclConfig().getPlainAccessConfigs()) {
            if (plainAccessConfig.getAccessKey().equals("rocketmqx")) {
                List<ResourceAndPerm> resourceAndPerms = plainAccessConfig.getResourcePerms();
                for (ResourceAndPerm resourceAndPerm1 : resourceAndPerms) {
                    if (resourceAndPerm1.getResource().equals("topicB")) {
                        Assert.assertEquals(resourceAndPerm1.getType(), ResourceType.TOPIC);
                        Assert.assertEquals(resourceAndPerm1.getNamespace(), "namespace1");
                        Assert.assertEquals(resourceAndPerm1.getPerm(), "PUB|SUB");
                        break;
                    }
                }
                break;
            }
        }
        plainAccessValidator.updateAclResourcePerms(accesskey, resourceAndPerm, OperationType.DELETE);
        for (PlainAccessConfig plainAccessConfig : plainAccessValidator.getAllAclConfig().getPlainAccessConfigs()) {
            if (plainAccessConfig.getAccessKey().equals("rocketmqx")) {
                Assert.assertEquals(plainAccessConfig.getResourcePerms().size(), 1);
                break;
            }
        }
        transport.delete();
        File bak = new File(fileName + ".bak");
        if (bak.exists()) {
            bak.delete();
        }
    }

    @Test
    public void updateAclNamespacePermsTest() throws IOException, InterruptedException {
        String fileName = System.getProperty("rocketmq.home.dir") + File.separator + "conf/acl/plain_acl_test.yml";
        File transport = new File(fileName);
        if (transport.exists()) {
            transport.delete();
        }
        transport.createNewFile();
        FileWriter writer = new FileWriter(transport);
        writer.write("accounts:\r\n");
        writer.write("- accessKey: rocketmqx\r\n");
        writer.write("  secretKey: 12345678\r\n");
        writer.write("  whiteRemoteAddress: 127.0.0.1\r\n");
        writer.write("  admin: false\r\n");
        writer.write("  resourcePerms:\r\n");
        writer.write("  - resource: topicA\r\n");
        writer.write("    type: TOPIC\r\n");
        writer.write("    namespace: namespace1\r\n");
        writer.write("    perm: PUB\r\n");
        writer.write("  namespacePerms:\r\n");
        writer.write("  - namespace: namespace1\r\n");
        writer.write("    topicPerm: DENY\r\n");
        writer.write("    groupPerm: DENY\r\n");
        writer.flush();
        writer.close();

        Thread.sleep(1000);

        String accesskey = "rocketmqx";
        List<NamespaceAndPerm> namespaceAndPermList = new ArrayList<>();
        NamespaceAndPerm namespaceAndPerm = new NamespaceAndPerm();
        namespaceAndPerm.setNamespace("namespace2");
        namespaceAndPerm.setTopicPerm("DENY");
        namespaceAndPerm.setGroupPerm("DENY");
        namespaceAndPermList.add(namespaceAndPerm);
        PlainAccessValidator plainAccessValidator = new PlainAccessValidator();
        plainAccessValidator.updateAclNamespacePerms(accesskey, namespaceAndPermList, OperationType.ADD);
        for (PlainAccessConfig plainAccessConfig : plainAccessValidator.getAllAclConfig().getPlainAccessConfigs()) {
            if (plainAccessConfig.getAccessKey().equals("rocketmqx")) {
                Assert.assertEquals(plainAccessConfig.getNamespacePerms().size(), 2);
                for (NamespaceAndPerm namespaceAndPerm1 : plainAccessConfig.getNamespacePerms()) {
                    if (namespaceAndPerm1.getNamespace().equals("namespace2")) {
                        Assert.assertEquals(namespaceAndPerm1.getTopicPerm(), "DENY");
                        Assert.assertEquals(namespaceAndPerm1.getGroupPerm(), "DENY");
                        break;
                    }
                }
                break;
            }
        }
        Iterator iterator = namespaceAndPermList.iterator();
        while (iterator.hasNext()) {
            NamespaceAndPerm cur = (NamespaceAndPerm)iterator.next();
            if (cur.getNamespace().equals("namespace2")) {
                iterator.remove();
                break;
            }
        }
        namespaceAndPerm.setTopicPerm("PUB");
        namespaceAndPermList.add(namespaceAndPerm);
        plainAccessValidator.updateAclNamespacePerms(accesskey, namespaceAndPermList, OperationType.UPDATE);
        for (PlainAccessConfig plainAccessConfig : plainAccessValidator.getAllAclConfig().getPlainAccessConfigs()) {
            if (plainAccessConfig.getAccessKey().equals("rocketmqx")) {
                Assert.assertEquals(plainAccessConfig.getNamespacePerms().size(), 2);
                for (NamespaceAndPerm namespaceAndPerm1 : plainAccessConfig.getNamespacePerms()) {
                    if (namespaceAndPerm1.getNamespace().equals("namespace2")) {
                        Assert.assertEquals(namespaceAndPerm1.getTopicPerm(), "PUB");
                        Assert.assertEquals(namespaceAndPerm1.getGroupPerm(), "DENY");
                        break;
                    }
                }
                break;
            }
        }
        plainAccessValidator.updateAclNamespacePerms(accesskey, namespaceAndPermList, OperationType.DELETE);
        for (PlainAccessConfig plainAccessConfig : plainAccessValidator.getAllAclConfig().getPlainAccessConfigs()) {
            if (plainAccessConfig.getAccessKey().equals("rocketmqx")) {
                Assert.assertEquals(plainAccessConfig.getNamespacePerms().size(), 1);
                for (NamespaceAndPerm namespaceAndPerm1 : plainAccessConfig.getNamespacePerms()) {
                    if (namespaceAndPerm1.getNamespace().equals("namespace1")) {
                        Assert.assertEquals(namespaceAndPerm1.getTopicPerm(), "DENY");
                        Assert.assertEquals(namespaceAndPerm1.getGroupPerm(), "DENY");
                        break;
                    }
                }
                break;
            }
        }
        transport.delete();
        File bak = new File(fileName + ".bak");
        if (bak.exists()) {
            bak.delete();
        }
    }

    @Test
    public void getConfigByAccessKeyTest() throws IOException, InterruptedException {
        String fileName = System.getProperty("rocketmq.home.dir") + File.separator + "conf/acl/plain_acl_test.yml";
        File transport = new File(fileName);
        if (transport.exists()) {
            transport.delete();
        }
        transport.createNewFile();
        FileWriter writer = new FileWriter(transport);
        writer.write("accounts:\r\n");
        writer.write("- accessKey: rocketmqx\r\n");
        writer.write("  secretKey: 12345678\r\n");
        writer.write("  whiteRemoteAddress: 127.0.0.1\r\n");
        writer.write("  admin: false\r\n");
        writer.write("  defaultTopicPerm: DENY\r\n");
        writer.write("  defaultGroupPerm: SUB\r\n");
        writer.write("  resourcePerms:\r\n");
        writer.write("  - resource: topicA\r\n");
        writer.write("    type: TOPIC\r\n");
        writer.write("    namespace: namespace1\r\n");
        writer.write("    perm: PUB\r\n");
        writer.write("  namespacePerms:\r\n");
        writer.write("  - namespace: namespace1\r\n");
        writer.write("    topicPerm: DENY\r\n");
        writer.write("    groupPerm: DENY\r\n");
        writer.flush();
        writer.close();

        Thread.sleep(1000);

        String accesskey = "rocketmqx";
        PlainAccessValidator plainAccessValidator = new PlainAccessValidator();
        PlainAccessConfig plainAccessConfig = plainAccessValidator.getConfigByAccessKey(accesskey);
        Assert.assertEquals(plainAccessConfig.getSecretKey(), "12345678");
        Assert.assertEquals(plainAccessConfig.getWhiteRemoteAddress(), "127.0.0.1");
        Assert.assertEquals(plainAccessConfig.isAdmin(), false);
        Assert.assertEquals(plainAccessConfig.getDefaultTopicPerm(), "DENY");
        Assert.assertEquals(plainAccessConfig.getDefaultGroupPerm(), "SUB");
        Assert.assertEquals(plainAccessConfig.getResourcePerms().size(), 1);
        Assert.assertEquals(plainAccessConfig.getResourcePerms().get(0).getResource(), "topicA");
        Assert.assertEquals(plainAccessConfig.getResourcePerms().get(0).getType(), ResourceType.TOPIC);
        Assert.assertEquals(plainAccessConfig.getResourcePerms().get(0).getNamespace(), "namespace1");
        Assert.assertEquals(plainAccessConfig.getResourcePerms().get(0).getPerm(), "PUB");
        Assert.assertEquals(plainAccessConfig.getNamespacePerms().size(), 1);
        Assert.assertEquals(plainAccessConfig.getNamespacePerms().get(0).getNamespace(), "namespace1");
        Assert.assertEquals(plainAccessConfig.getNamespacePerms().get(0).getTopicPerm(), "DENY");
        Assert.assertEquals(plainAccessConfig.getNamespacePerms().get(0).getGroupPerm(), "DENY");

        transport.delete();
        File bak = new File(fileName + ".bak");
        if (bak.exists()) {
            bak.delete();
        }
    }
}
