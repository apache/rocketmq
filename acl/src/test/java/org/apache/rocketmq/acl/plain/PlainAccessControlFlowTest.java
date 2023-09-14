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

import org.apache.rocketmq.acl.common.AclClientRPCHook;
import org.apache.rocketmq.acl.common.AclConstants;
import org.apache.rocketmq.acl.common.AclException;
import org.apache.rocketmq.acl.common.AclUtils;
import org.apache.rocketmq.acl.common.SessionCredentials;
import org.apache.rocketmq.common.AclConfig;
import org.apache.rocketmq.common.PlainAccessConfig;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.RequestCode;
import org.apache.rocketmq.remoting.protocol.header.PullMessageRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.SendMessageRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.SendMessageRequestHeaderV2;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

/**
 * <p> In this class, we'll test the following scenarios, each containing several consecutive operations on ACL,
 * <p> like updating and deleting ACL, changing config files and checking validations.
 * <p> Case 1: Only conf/plain_acl.yml exists;
 * <p> Case 2: Only conf/acl/plain_acl.yml exists;
 * <p> Case 3: Both conf/plain_acl.yml and conf/acl/plain_acl.yml exists.
 */
public class PlainAccessControlFlowTest {
    public static final String DEFAULT_TOPIC = "topic-acl";

    public static final String DEFAULT_GROUP = "GID_acl";

    public static final String DEFAULT_PRODUCER_AK = "ak11111";
    public static final String DEFAULT_PRODUCER_SK = "1234567";

    public static final String DEFAULT_CONSUMER_SK = "7654321";
    public static final String DEFAULT_CONSUMER_AK = "ak22222";

    public static final String DEFAULT_GLOBAL_WHITE_ADDR = "172.16.123.123";
    public static final List<String> DEFAULT_GLOBAL_WHITE_ADDRS_LIST = Collections.singletonList(DEFAULT_GLOBAL_WHITE_ADDR);

    @Test
    public void testEmptyAclFolderCase() throws NoSuchFieldException, IllegalAccessException,
        IOException {
        String folder = "empty_acl_folder_conf";
        File home = AclTestHelper.copyResources(folder);
        System.setProperty("rocketmq.home.dir", home.getAbsolutePath());
        PlainAccessValidator plainAccessValidator = new PlainAccessValidator();
        checkDefaultAclFileExists();
        testValidationAfterConsecutiveUpdates(plainAccessValidator);
        testValidationAfterConfigFileChanged(plainAccessValidator);
        AclTestHelper.recursiveDelete(home);
    }

    @Test
    public void testOnlyAclFolderCase() throws NoSuchFieldException, IllegalAccessException, IOException {
        String folder = "only_acl_folder_conf";
        File home = AclTestHelper.copyResources(folder);
        System.setProperty("rocketmq.home.dir", home.getAbsolutePath());
        PlainAccessValidator plainAccessValidator = new PlainAccessValidator();
        checkDefaultAclFileExists();
        testValidationAfterConsecutiveUpdates(plainAccessValidator);
        testValidationAfterConfigFileChanged(plainAccessValidator);
        AclTestHelper.recursiveDelete(home);
    }

    @Test
    public void testBothAclFileAndFolderCase() throws NoSuchFieldException, IllegalAccessException,
        IOException {
        String folder = "both_acl_file_folder_conf";
        File root = AclTestHelper.copyResources(folder);
        System.setProperty("rocketmq.home.dir", root.getAbsolutePath());
        PlainAccessValidator plainAccessValidator = new PlainAccessValidator();
        checkDefaultAclFileExists();
        testValidationAfterConsecutiveUpdates(plainAccessValidator);
        testValidationAfterConfigFileChanged(plainAccessValidator);
        AclTestHelper.recursiveDelete(root);
    }

    private void testValidationAfterConfigFileChanged(
        PlainAccessValidator plainAccessValidator) throws NoSuchFieldException, IllegalAccessException {
        PlainAccessConfig producerAccessConfig = generateProducerAccessConfig();
        PlainAccessConfig consumerAccessConfig = generateConsumerAccessConfig();
        List<PlainAccessConfig> plainAccessConfigList = new LinkedList<>();
        plainAccessConfigList.add(producerAccessConfig);
        plainAccessConfigList.add(consumerAccessConfig);
        PlainAccessData ymlMap = new PlainAccessData();
        ymlMap.setAccounts(plainAccessConfigList);

        // write prepared PlainAccessConfigs to file
        final String aclConfigFile = System.getProperty("rocketmq.home.dir") + File.separator + "conf/plain_acl.yml";
        AclUtils.writeDataObject(aclConfigFile, ymlMap);

        loadConfigFile(plainAccessValidator, aclConfigFile);

        // check if added successfully
        final AclConfig allAclConfig = plainAccessValidator.getAllAclConfig();
        final List<PlainAccessConfig> plainAccessConfigs = allAclConfig.getPlainAccessConfigs();
        checkPlainAccessConfig(producerAccessConfig, plainAccessConfigs);
        checkPlainAccessConfig(consumerAccessConfig, plainAccessConfigs);

        //delete consumer account
        plainAccessConfigList.remove(consumerAccessConfig);
        AclUtils.writeDataObject(aclConfigFile, ymlMap);

        loadConfigFile(plainAccessValidator, aclConfigFile);

        // sending messages will be successful using prepared credentials
        SessionCredentials producerCredential = new SessionCredentials(DEFAULT_PRODUCER_AK, DEFAULT_PRODUCER_SK);
        AclClientRPCHook producerHook = new AclClientRPCHook(producerCredential);
        validateSendMessage(RequestCode.SEND_MESSAGE, DEFAULT_TOPIC, producerHook, "", plainAccessValidator);
        validateSendMessage(RequestCode.SEND_MESSAGE_V2, DEFAULT_TOPIC, producerHook, "", plainAccessValidator);

        // consuming messages will be failed for account has been deleted
        SessionCredentials consumerCredential = new SessionCredentials(DEFAULT_CONSUMER_AK, DEFAULT_CONSUMER_SK);
        AclClientRPCHook consumerHook = new AclClientRPCHook(consumerCredential);
        boolean isConsumeFailed = false;
        try {
            validatePullMessage(DEFAULT_TOPIC, DEFAULT_GROUP, consumerHook, "", plainAccessValidator);
        } catch (AclException e) {
            isConsumeFailed = true;
        }
        Assert.assertTrue("Message should not be consumed after account deleted", isConsumeFailed);

    }

    private void testValidationAfterConsecutiveUpdates(
        PlainAccessValidator plainAccessValidator) throws NoSuchFieldException, IllegalAccessException {
        PlainAccessConfig producerAccessConfig = generateProducerAccessConfig();
        plainAccessValidator.updateAccessConfig(producerAccessConfig);

        PlainAccessConfig consumerAccessConfig = generateConsumerAccessConfig();
        plainAccessValidator.updateAccessConfig(consumerAccessConfig);

        plainAccessValidator.updateGlobalWhiteAddrsConfig(DEFAULT_GLOBAL_WHITE_ADDRS_LIST, null);

        // check if the above config updated successfully
        final AclConfig allAclConfig = plainAccessValidator.getAllAclConfig();
        final List<PlainAccessConfig> plainAccessConfigs = allAclConfig.getPlainAccessConfigs();
        checkPlainAccessConfig(producerAccessConfig, plainAccessConfigs);
        checkPlainAccessConfig(consumerAccessConfig, plainAccessConfigs);

        Assert.assertEquals(DEFAULT_GLOBAL_WHITE_ADDRS_LIST, allAclConfig.getGlobalWhiteAddrs());

        // check sending and consuming messages
        SessionCredentials producerCredential = new SessionCredentials(DEFAULT_PRODUCER_AK, DEFAULT_PRODUCER_SK);
        AclClientRPCHook producerHook = new AclClientRPCHook(producerCredential);
        validateSendMessage(RequestCode.SEND_MESSAGE, DEFAULT_TOPIC, producerHook, "", plainAccessValidator);
        validateSendMessage(RequestCode.SEND_MESSAGE_V2, DEFAULT_TOPIC, producerHook, "", plainAccessValidator);

        SessionCredentials consumerCredential = new SessionCredentials(DEFAULT_CONSUMER_AK, DEFAULT_CONSUMER_SK);
        AclClientRPCHook consumerHook = new AclClientRPCHook(consumerCredential);
        validatePullMessage(DEFAULT_TOPIC, DEFAULT_GROUP, consumerHook, "", plainAccessValidator);

        // load from file
        loadConfigFile(plainAccessValidator,
            System.getProperty("rocketmq.home.dir") + File.separator + "conf/plain_acl.yml");
        SessionCredentials unmatchedCredential = new SessionCredentials("non_exists_sk", "non_exists_sk");
        AclClientRPCHook dummyHook = new AclClientRPCHook(unmatchedCredential);
        validateSendMessage(RequestCode.SEND_MESSAGE, DEFAULT_TOPIC, dummyHook, DEFAULT_GLOBAL_WHITE_ADDR, plainAccessValidator);
        validateSendMessage(RequestCode.SEND_MESSAGE_V2, DEFAULT_TOPIC, dummyHook, DEFAULT_GLOBAL_WHITE_ADDR, plainAccessValidator);
        validatePullMessage(DEFAULT_TOPIC, DEFAULT_GROUP, dummyHook, DEFAULT_GLOBAL_WHITE_ADDR, plainAccessValidator);

        //recheck after reloading
        validateSendMessage(RequestCode.SEND_MESSAGE, DEFAULT_TOPIC, producerHook, "", plainAccessValidator);
        validateSendMessage(RequestCode.SEND_MESSAGE_V2, DEFAULT_TOPIC, producerHook, "", plainAccessValidator);
        validatePullMessage(DEFAULT_TOPIC, DEFAULT_GROUP, consumerHook, "", plainAccessValidator);

    }

    private void loadConfigFile(PlainAccessValidator plainAccessValidator,
        String configFileName) throws NoSuchFieldException, IllegalAccessException {
        Class<PlainAccessValidator> clazz = PlainAccessValidator.class;
        Field f = clazz.getDeclaredField("aclPlugEngine");
        f.setAccessible(true);
        PlainPermissionManager aclPlugEngine = (PlainPermissionManager) f.get(plainAccessValidator);
        aclPlugEngine.load(configFileName);
    }

    private PlainAccessConfig generateConsumerAccessConfig() {
        PlainAccessConfig plainAccessConfig2 = new PlainAccessConfig();
        plainAccessConfig2.setAccessKey(DEFAULT_CONSUMER_AK);
        plainAccessConfig2.setSecretKey(DEFAULT_CONSUMER_SK);
        plainAccessConfig2.setAdmin(false);
        plainAccessConfig2.setDefaultTopicPerm(AclConstants.DENY);
        plainAccessConfig2.setDefaultGroupPerm(AclConstants.DENY);
        plainAccessConfig2.setTopicPerms(Collections.singletonList(DEFAULT_TOPIC + "=" + AclConstants.SUB));
        plainAccessConfig2.setGroupPerms(Collections.singletonList(DEFAULT_GROUP + "=" + AclConstants.SUB));
        return plainAccessConfig2;
    }

    private PlainAccessConfig generateProducerAccessConfig() {
        PlainAccessConfig plainAccessConfig = new PlainAccessConfig();
        plainAccessConfig.setAccessKey(DEFAULT_PRODUCER_AK);
        plainAccessConfig.setSecretKey(DEFAULT_PRODUCER_SK);
        plainAccessConfig.setAdmin(false);
        plainAccessConfig.setDefaultTopicPerm(AclConstants.DENY);
        plainAccessConfig.setDefaultGroupPerm(AclConstants.DENY);
        plainAccessConfig.setTopicPerms(Collections.singletonList(DEFAULT_TOPIC + "=" + AclConstants.PUB));
        return plainAccessConfig;
    }

    public void validatePullMessage(String topic,
        String group,
        AclClientRPCHook aclClientRPCHook,
        String remoteAddr,
        PlainAccessValidator plainAccessValidator) {
        PullMessageRequestHeader pullMessageRequestHeader = new PullMessageRequestHeader();
        pullMessageRequestHeader.setTopic(topic);
        pullMessageRequestHeader.setConsumerGroup(group);
        RemotingCommand remotingCommand = RemotingCommand.createRequestCommand(RequestCode.PULL_MESSAGE,
            pullMessageRequestHeader);
        aclClientRPCHook.doBeforeRequest(remoteAddr, remotingCommand);
        ByteBuffer buf = remotingCommand.encodeHeader();
        buf.getInt();
        buf = ByteBuffer.allocate(buf.limit() - buf.position()).put(buf);
        buf.position(0);
        try {
            PlainAccessResource accessResource = (PlainAccessResource) plainAccessValidator.parse(
                RemotingCommand.decode(buf), remoteAddr);
            plainAccessValidator.validate(accessResource);
        } catch (RemotingCommandException e) {
            e.printStackTrace();
            Assert.fail("Should not throw RemotingCommandException");
        }
    }

    public void validateSendMessage(int requestCode,
        String topic,
        AclClientRPCHook aclClientRPCHook,
        String remoteAddr,
        PlainAccessValidator plainAccessValidator) {
        SendMessageRequestHeader messageRequestHeader = new SendMessageRequestHeader();
        messageRequestHeader.setTopic(topic);
        RemotingCommand remotingCommand;
        if (RequestCode.SEND_MESSAGE == requestCode) {
            remotingCommand = RemotingCommand.createRequestCommand(RequestCode.SEND_MESSAGE, messageRequestHeader);
        } else {
            remotingCommand = RemotingCommand.createRequestCommand(RequestCode.SEND_MESSAGE_V2,
                SendMessageRequestHeaderV2.createSendMessageRequestHeaderV2(messageRequestHeader));
        }

        aclClientRPCHook.doBeforeRequest(remoteAddr, remotingCommand);

        ByteBuffer buf = remotingCommand.encodeHeader();
        buf.getInt();
        buf = ByteBuffer.allocate(buf.limit() - buf.position()).put(buf);
        buf.position(0);
        try {
            PlainAccessResource accessResource = (PlainAccessResource) plainAccessValidator.parse(
                RemotingCommand.decode(buf), remoteAddr);
            plainAccessValidator.validate(accessResource);
        } catch (RemotingCommandException e) {
            e.printStackTrace();
            Assert.fail("Should not throw RemotingCommandException");
        }
    }

    private void checkPlainAccessConfig(final PlainAccessConfig plainAccessConfig,
        final List<PlainAccessConfig> plainAccessConfigs) {
        for (PlainAccessConfig config : plainAccessConfigs) {
            if (config.getAccessKey().equals(plainAccessConfig.getAccessKey())) {
                Assert.assertEquals(plainAccessConfig.getSecretKey(), config.getSecretKey());
                Assert.assertEquals(plainAccessConfig.isAdmin(), config.isAdmin());
                Assert.assertEquals(plainAccessConfig.getDefaultGroupPerm(), config.getDefaultGroupPerm());
                Assert.assertEquals(plainAccessConfig.getDefaultGroupPerm(), config.getDefaultGroupPerm());
                Assert.assertEquals(plainAccessConfig.getWhiteRemoteAddress(), config.getWhiteRemoteAddress());
                if (null != plainAccessConfig.getTopicPerms()) {
                    Assert.assertNotNull(config.getTopicPerms());
                    Assert.assertTrue(config.getTopicPerms().containsAll(plainAccessConfig.getTopicPerms()));
                }
                if (null != plainAccessConfig.getGroupPerms()) {
                    Assert.assertNotNull(config.getGroupPerms());
                    Assert.assertTrue(config.getGroupPerms().containsAll(plainAccessConfig.getGroupPerms()));
                }
            }
        }
    }

    private void checkDefaultAclFileExists() {
        boolean isExists = Files.exists(Paths.get(System.getProperty("rocketmq.home.dir")
            + File.separator + "conf" + File.separator + "plain_acl.yml"));
        Assert.assertTrue("default acl config file should exist", isExists);
    }

}
