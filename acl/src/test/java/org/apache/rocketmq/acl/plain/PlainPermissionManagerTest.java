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

import com.google.common.base.Joiner;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.rocketmq.acl.common.AclConstants;
import org.apache.rocketmq.acl.common.AclException;
import org.apache.rocketmq.acl.common.AclUtils;
import org.apache.rocketmq.acl.common.Permission;
import org.apache.rocketmq.common.AclConfig;
import org.apache.rocketmq.common.PlainAccessConfig;
import org.apache.rocketmq.remoting.protocol.DataVersion;
import org.assertj.core.api.Assertions;
import org.assertj.core.util.Lists;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class PlainPermissionManagerTest {

    PlainPermissionManager plainPermissionManager;
    PlainAccessResource pubPlainAccessResource;
    PlainAccessResource subPlainAccessResource;
    PlainAccessResource anyPlainAccessResource;
    PlainAccessResource denyPlainAccessResource;
    PlainAccessResource plainAccessResource = new PlainAccessResource();
    PlainAccessConfig plainAccessConfig = new PlainAccessConfig();
    Set<Integer> adminCode = new HashSet<>();

    private static final String DEFAULT_TOPIC = "topic-acl";

    private File confHome;

    @Before
    public void init() throws NoSuchFieldException, SecurityException, IOException {
        // UPDATE_AND_CREATE_TOPIC
        adminCode.add(17);
        // UPDATE_BROKER_CONFIG
        adminCode.add(25);
        // DELETE_TOPIC_IN_BROKER
        adminCode.add(215);
        // UPDATE_AND_CREATE_SUBSCRIPTIONGROUP
        adminCode.add(200);
        // DELETE_SUBSCRIPTIONGROUP
        adminCode.add(207);

        pubPlainAccessResource = clonePlainAccessResource(Permission.PUB);
        subPlainAccessResource = clonePlainAccessResource(Permission.SUB);
        anyPlainAccessResource = clonePlainAccessResource(Permission.ANY);
        denyPlainAccessResource = clonePlainAccessResource(Permission.DENY);

        String folder = "conf";
        confHome = AclTestHelper.copyResources(folder, true);
        System.setProperty("rocketmq.home.dir", confHome.getAbsolutePath());
        plainPermissionManager = new PlainPermissionManager();
    }

    public PlainAccessResource clonePlainAccessResource(byte perm) {
        PlainAccessResource painAccessResource = new PlainAccessResource();
        painAccessResource.setAccessKey("RocketMQ");
        painAccessResource.setSecretKey("12345678");
        painAccessResource.setWhiteRemoteAddress("127.0." + perm + ".*");
        painAccessResource.setDefaultGroupPerm(perm);
        painAccessResource.setDefaultTopicPerm(perm);
        painAccessResource.addResourceAndPerm(PlainAccessResource.getRetryTopic("groupA"), Permission.PUB);
        painAccessResource.addResourceAndPerm(PlainAccessResource.getRetryTopic("groupB"), Permission.SUB);
        painAccessResource.addResourceAndPerm(PlainAccessResource.getRetryTopic("groupC"), Permission.ANY);
        painAccessResource.addResourceAndPerm(PlainAccessResource.getRetryTopic("groupD"), Permission.DENY);

        painAccessResource.addResourceAndPerm("topicA", Permission.PUB);
        painAccessResource.addResourceAndPerm("topicB", Permission.SUB);
        painAccessResource.addResourceAndPerm("topicC", Permission.ANY);
        painAccessResource.addResourceAndPerm("topicD", Permission.DENY);
        return painAccessResource;
    }

    @Test
    public void buildPlainAccessResourceTest() {
        PlainAccessResource plainAccessResource = null;
        PlainAccessConfig plainAccess = new PlainAccessConfig();

        plainAccess.setAccessKey("RocketMQ");
        plainAccess.setSecretKey("12345678");
        plainAccessResource = plainPermissionManager.buildPlainAccessResource(plainAccess);
        Assert.assertEquals(plainAccessResource.getAccessKey(), "RocketMQ");
        Assert.assertEquals(plainAccessResource.getSecretKey(), "12345678");

        plainAccess.setWhiteRemoteAddress("127.0.0.1");
        plainAccessResource = plainPermissionManager.buildPlainAccessResource(plainAccess);
        Assert.assertEquals(plainAccessResource.getWhiteRemoteAddress(), "127.0.0.1");

        plainAccess.setAdmin(true);
        plainAccessResource = plainPermissionManager.buildPlainAccessResource(plainAccess);
        Assert.assertEquals(plainAccessResource.isAdmin(), true);

        List<String> groups = new ArrayList<>();
        groups.add("groupA=DENY");
        groups.add("groupB=PUB|SUB");
        groups.add("groupC=PUB");
        plainAccess.setGroupPerms(groups);
        plainAccessResource = plainPermissionManager.buildPlainAccessResource(plainAccess);
        Map<String, Byte> resourcePermMap = plainAccessResource.getResourcePermMap();
        Assert.assertEquals(resourcePermMap.size(), 3);

        Assert.assertEquals(resourcePermMap.get(PlainAccessResource.getRetryTopic("groupA")).byteValue(), Permission.DENY);
        Assert.assertEquals(resourcePermMap.get(PlainAccessResource.getRetryTopic("groupB")).byteValue(), Permission.PUB | Permission.SUB);
        Assert.assertEquals(resourcePermMap.get(PlainAccessResource.getRetryTopic("groupC")).byteValue(), Permission.PUB);

        List<String> topics = new ArrayList<>();
        topics.add("topicA=DENY");
        topics.add("topicB=PUB|SUB");
        topics.add("topicC=PUB");
        plainAccess.setTopicPerms(topics);
        plainAccessResource = plainPermissionManager.buildPlainAccessResource(plainAccess);
        resourcePermMap = plainAccessResource.getResourcePermMap();
        Assert.assertEquals(resourcePermMap.size(), 6);

        Assert.assertEquals(resourcePermMap.get("topicA").byteValue(), Permission.DENY);
        Assert.assertEquals(resourcePermMap.get("topicB").byteValue(), Permission.PUB | Permission.SUB);
        Assert.assertEquals(resourcePermMap.get("topicC").byteValue(), Permission.PUB);
    }

    @Test(expected = AclException.class)
    public void checkPermAdmin() {
        PlainAccessResource plainAccessResource = new PlainAccessResource();
        plainAccessResource.setRequestCode(17);
        plainPermissionManager.checkPerm(plainAccessResource, pubPlainAccessResource);
    }

    @Test
    public void checkPerm() {

        PlainAccessResource plainAccessResource = new PlainAccessResource();
        plainAccessResource.addResourceAndPerm("topicA", Permission.PUB);
        plainPermissionManager.checkPerm(plainAccessResource, pubPlainAccessResource);
        plainAccessResource.addResourceAndPerm("topicB", Permission.SUB);
        plainPermissionManager.checkPerm(plainAccessResource, anyPlainAccessResource);

        plainAccessResource = new PlainAccessResource();
        plainAccessResource.addResourceAndPerm("topicB", Permission.SUB);
        plainPermissionManager.checkPerm(plainAccessResource, subPlainAccessResource);
        plainAccessResource.addResourceAndPerm("topicA", Permission.PUB);
        plainPermissionManager.checkPerm(plainAccessResource, anyPlainAccessResource);

    }

    @Test(expected = AclException.class)
    public void checkErrorPermDefaultValueNotMatch() {

        plainAccessResource = new PlainAccessResource();
        plainAccessResource.addResourceAndPerm("topicF", Permission.PUB);
        plainPermissionManager.checkPerm(plainAccessResource, subPlainAccessResource);
    }

    @Test(expected = AclException.class)
    public void accountNullTest() {
        plainAccessConfig.setAccessKey(null);
        plainPermissionManager.buildPlainAccessResource(plainAccessConfig);
    }

    @Test(expected = AclException.class)
    public void accountThanTest() {
        plainAccessConfig.setAccessKey("123");
        plainPermissionManager.buildPlainAccessResource(plainAccessConfig);
    }

    @Test(expected = AclException.class)
    public void passWordtNullTest() {
        plainAccessConfig.setAccessKey(null);
        plainPermissionManager.buildPlainAccessResource(plainAccessConfig);
    }

    @Test(expected = AclException.class)
    public void passWordThanTest() {
        plainAccessConfig.setSecretKey("123");
        plainPermissionManager.buildPlainAccessResource(plainAccessConfig);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void cleanAuthenticationInfoTest() throws IllegalAccessException {
        // PlainPermissionManager.addPlainAccessResource(plainAccessResource);
        Map<String, Map<String, PlainAccessResource>> plainAccessResourceMap = (Map<String, Map<String, PlainAccessResource>>) FieldUtils.readDeclaredField(plainPermissionManager, "aclPlainAccessResourceMap", true);
        Assert.assertFalse(plainAccessResourceMap.isEmpty());

        plainPermissionManager.clearPermissionInfo();
        plainAccessResourceMap = (Map<String, Map<String, PlainAccessResource>>) FieldUtils.readDeclaredField(plainPermissionManager, "aclPlainAccessResourceMap", true);
        Assert.assertTrue(plainAccessResourceMap.isEmpty());
    }

    @Test
    public void isWatchStartTest() {

        PlainPermissionManager plainPermissionManager = new PlainPermissionManager();
        Assert.assertTrue(plainPermissionManager.isWatchStart());
    }

    @Test
    public void testWatch() throws IOException, IllegalAccessException, InterruptedException {
        String fileName =  Joiner.on(File.separator).join(new String[]{System.getProperty("rocketmq.home.dir"), "conf", "acl", "plain_acl_test.yml"});
        File transport = new File(fileName);
        transport.delete();
        transport.createNewFile();
        FileWriter writer = new FileWriter(transport);
        writer.write("accounts:\r\n");
        writer.write("- accessKey: watchrocketmqx\r\n");
        writer.write("  secretKey: 12345678\r\n");
        writer.write("  whiteRemoteAddress: 127.0.0.1\r\n");
        writer.write("  admin: true\r\n");
        writer.flush();
        writer.close();

        Thread.sleep(1000);

        PlainPermissionManager plainPermissionManager = new PlainPermissionManager();
        Assert.assertTrue(plainPermissionManager.isWatchStart());

        Map<String, String> accessKeyTable = (Map<String, String>) FieldUtils.readDeclaredField(plainPermissionManager, "accessKeyTable", true);
        String aclFileName = accessKeyTable.get("watchrocketmqx");
        {
            Map<String, Map<String, PlainAccessResource>> plainAccessResourceMap = (Map<String, Map<String, PlainAccessResource>>) FieldUtils.readDeclaredField(plainPermissionManager, "aclPlainAccessResourceMap", true);
            PlainAccessResource accessResource = plainAccessResourceMap.get(aclFileName).get("watchrocketmqx");
            Assert.assertNotNull(accessResource);
            Assert.assertEquals(accessResource.getSecretKey(), "12345678");
            Assert.assertTrue(accessResource.isAdmin());

        }

        PlainAccessData updatedMap = AclUtils.getYamlDataObject(fileName, PlainAccessData.class);
        List<PlainAccessConfig> accounts = updatedMap.getAccounts();
        accounts.get(0).setAccessKey("watchrocketmq1y");
        accounts.get(0).setSecretKey("88888888");
        accounts.get(0).setAdmin(false);
        // Update file and flush to yaml file
        AclUtils.writeDataObject(fileName, updatedMap);

        Thread.sleep(10000);
        {
            Map<String, Map<String, PlainAccessResource>> plainAccessResourceMap = (Map<String, Map<String, PlainAccessResource>>) FieldUtils.readDeclaredField(plainPermissionManager, "aclPlainAccessResourceMap", true);
            PlainAccessResource accessResource = plainAccessResourceMap.get(aclFileName).get("watchrocketmq1y");
            Assert.assertNotNull(accessResource);
            Assert.assertEquals(accessResource.getSecretKey(), "88888888");
            Assert.assertFalse(accessResource.isAdmin());

        }
        transport.delete();
    }

    @Test
    public void updateAccessConfigTest() {
        Assert.assertThrows(AclException.class, () -> plainPermissionManager.updateAccessConfig(null));

        plainAccessConfig.setAccessKey("admin_test");
        // Invalid parameter
        plainAccessConfig.setSecretKey("123456");
        plainAccessConfig.setAdmin(true);
        Assert.assertThrows(AclException.class, () -> plainPermissionManager.updateAccessConfig(plainAccessConfig));

        plainAccessConfig.setSecretKey("12345678");
        // Invalid parameter
        plainAccessConfig.setGroupPerms(Lists.newArrayList("groupA!SUB"));
        Assert.assertThrows(AclException.class, () -> plainPermissionManager.updateAccessConfig(plainAccessConfig));

        // first update
        plainAccessConfig.setGroupPerms(Lists.newArrayList("groupA=SUB"));
        plainPermissionManager.updateAccessConfig(plainAccessConfig);

        // second update
        plainAccessConfig.setTopicPerms(Lists.newArrayList("topicA=SUB"));
        plainPermissionManager.updateAccessConfig(plainAccessConfig);
    }

    @Test
    public void getAllAclFilesTest() {
        final List<String> notExistList = plainPermissionManager.getAllAclFiles("aa/bb");
        Assertions.assertThat(notExistList).isEmpty();
        final List<String> files = plainPermissionManager.getAllAclFiles(confHome.getAbsolutePath());
        Assertions.assertThat(files).isNotEmpty();
    }

    @Test
    public void loadTest() {
        plainPermissionManager.load();
        final Map<String, DataVersion> map = plainPermissionManager.getDataVersionMap();
        Assertions.assertThat(map).isNotEmpty();
    }

    @Test
    public void updateAclConfigFileVersionTest() {
        String aclFileName = "test_plain_acl";
        PlainAccessData updateAclConfigMap = new PlainAccessData();
        List<PlainAccessData.DataVersion> versionElement = new ArrayList<>();
        PlainAccessData.DataVersion accountsMap = new PlainAccessData.DataVersion();
        accountsMap.setCounter(1);
        accountsMap.setTimestamp(System.currentTimeMillis());
        versionElement.add(accountsMap);

        updateAclConfigMap.setDataVersion(versionElement);
        final PlainAccessData map = plainPermissionManager.updateAclConfigFileVersion(aclFileName, updateAclConfigMap);
        final List<PlainAccessData.DataVersion> version = map.getDataVersion();
        Assert.assertEquals(2L, version.get(0).getCounter());
    }

    @Test
    public void createAclAccessConfigMapTest() {
        PlainAccessConfig existedAccountMap = new PlainAccessConfig();
        plainAccessConfig.setAccessKey("admin123");
        plainAccessConfig.setSecretKey("12345678");
        plainAccessConfig.setWhiteRemoteAddress("192.168.1.1");
        plainAccessConfig.setAdmin(false);
        plainAccessConfig.setDefaultGroupPerm(AclConstants.SUB_PUB);
        plainAccessConfig.setTopicPerms(Arrays.asList(DEFAULT_TOPIC + "=" + AclConstants.PUB));
        plainAccessConfig.setGroupPerms(Lists.newArrayList("groupA=SUB"));

        final PlainAccessConfig map = plainPermissionManager.createAclAccessConfigMap(existedAccountMap, plainAccessConfig);
        Assert.assertEquals(AclConstants.SUB_PUB, map.getDefaultGroupPerm());
        Assert.assertEquals("groupA=SUB", map.getGroupPerms().get(0));
        Assert.assertEquals("12345678", map.getSecretKey());
        Assert.assertEquals("admin123", map.getAccessKey());
        Assert.assertEquals("192.168.1.1", map.getWhiteRemoteAddress());
        Assert.assertEquals("topic-acl=PUB", map.getTopicPerms().get(0));
        Assert.assertEquals(false, map.isAdmin());
    }

    @Test
    public void deleteAccessConfigTest() throws InterruptedException {
        // delete not exist accessConfig
        final boolean flag1 = plainPermissionManager.deleteAccessConfig("test_delete");
        assert !flag1;

        plainAccessConfig.setAccessKey("test_delete");
        plainAccessConfig.setSecretKey("12345678");
        plainAccessConfig.setWhiteRemoteAddress("192.168.1.1");
        plainAccessConfig.setAdmin(false);
        plainAccessConfig.setDefaultGroupPerm(AclConstants.SUB_PUB);
        plainAccessConfig.setTopicPerms(Arrays.asList(DEFAULT_TOPIC + "=" + AclConstants.PUB));
        plainAccessConfig.setGroupPerms(Lists.newArrayList("groupA=SUB"));
        plainPermissionManager.updateAccessConfig(plainAccessConfig);

        //delete existed accessConfig
        final boolean flag2 = plainPermissionManager.deleteAccessConfig("test_delete");
        assert flag2;

    }

    @Test
    public void updateGlobalWhiteAddrsConfigTest() {
        final boolean flag = plainPermissionManager.updateGlobalWhiteAddrsConfig(Lists.newArrayList("192.168.1.2"));
        assert flag;
        final AclConfig config = plainPermissionManager.getAllAclConfig();
        Assert.assertEquals(true, config.getGlobalWhiteAddrs().contains("192.168.1.2"));
    }

}
