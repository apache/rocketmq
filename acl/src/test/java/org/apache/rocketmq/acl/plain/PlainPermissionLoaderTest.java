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
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.rocketmq.acl.common.AclException;
import org.apache.rocketmq.acl.common.Permission;
import org.apache.rocketmq.acl.plain.PlainPermissionLoader.PlainAccessConfig;
import org.apache.rocketmq.common.UtilAll;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class PlainPermissionLoaderTest {

    PlainPermissionLoader plainPermissionLoader;
    PlainAccessResource PUBPlainAccessResource;
    PlainAccessResource SUBPlainAccessResource;
    PlainAccessResource ANYPlainAccessResource;
    PlainAccessResource DENYPlainAccessResource;
    PlainAccessResource plainAccessResource = new PlainAccessResource();
    PlainAccessConfig plainAccessConfig = new PlainAccessConfig();
    PlainAccessResource plainAccessResourceTwo = new PlainAccessResource();
    Set<Integer> adminCode = new HashSet<>();

    @Before
    public void init() throws NoSuchFieldException, SecurityException, IOException {
        //  UPDATE_AND_CREATE_TOPIC
        adminCode.add(17);
        //  UPDATE_BROKER_CONFIG
        adminCode.add(25);
        //  DELETE_TOPIC_IN_BROKER
        adminCode.add(215);
        // UPDATE_AND_CREATE_SUBSCRIPTIONGROUP
        adminCode.add(200);
        // DELETE_SUBSCRIPTIONGROUP
        adminCode.add(207);

        PUBPlainAccessResource = clonePlainAccessResource(Permission.PUB);
        SUBPlainAccessResource = clonePlainAccessResource(Permission.SUB);
        ANYPlainAccessResource = clonePlainAccessResource(Permission.ANY);
        DENYPlainAccessResource = clonePlainAccessResource(Permission.DENY);

        System.setProperty("rocketmq.home.dir", "src/test/resources");
        System.setProperty("rocketmq.acl.plain.file", "/conf/plain_acl.yml");
        plainPermissionLoader = new PlainPermissionLoader();

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
        plainAccessResource = plainPermissionLoader.buildPlainAccessResource(plainAccess);
        Assert.assertEquals(plainAccessResource.getAccessKey(), "RocketMQ");
        Assert.assertEquals(plainAccessResource.getSecretKey(), "12345678");

        plainAccess.setWhiteRemoteAddress("127.0.0.1");
        plainAccessResource = plainPermissionLoader.buildPlainAccessResource(plainAccess);
        Assert.assertEquals(plainAccessResource.getWhiteRemoteAddress(), "127.0.0.1");

        plainAccess.setAdmin(true);
        plainAccessResource = plainPermissionLoader.buildPlainAccessResource(plainAccess);
        Assert.assertEquals(plainAccessResource.isAdmin(), true);

        List<String> groups = new ArrayList<String>();
        groups.add("groupA=DENY");
        groups.add("groupB=PUB|SUB");
        groups.add("groupC=PUB");
        plainAccess.setGroupPerms(groups);
        plainAccessResource = plainPermissionLoader.buildPlainAccessResource(plainAccess);
        Map<String, Byte> resourcePermMap = plainAccessResource.getResourcePermMap();
        Assert.assertEquals(resourcePermMap.size(), 3);

        Assert.assertEquals(resourcePermMap.get(PlainAccessResource.getRetryTopic("groupA")).byteValue(), Permission.DENY);
        Assert.assertEquals(resourcePermMap.get(PlainAccessResource.getRetryTopic("groupB")).byteValue(), Permission.PUB|Permission.SUB);
        Assert.assertEquals(resourcePermMap.get(PlainAccessResource.getRetryTopic("groupC")).byteValue(), Permission.PUB);

        List<String> topics = new ArrayList<String>();
        topics.add("topicA=DENY");
        topics.add("topicB=PUB|SUB");
        topics.add("topicC=PUB");
        plainAccess.setTopicPerms(topics);
        plainAccessResource = plainPermissionLoader.buildPlainAccessResource(plainAccess);
        resourcePermMap = plainAccessResource.getResourcePermMap();
        Assert.assertEquals(resourcePermMap.size(), 6);

        Assert.assertEquals(resourcePermMap.get("topicA").byteValue(), Permission.DENY);
        Assert.assertEquals(resourcePermMap.get("topicB").byteValue(), Permission.PUB|Permission.SUB);
        Assert.assertEquals(resourcePermMap.get("topicC").byteValue(), Permission.PUB);
    }

    @Test(expected = AclException.class)
    public void checkPermAdmin() {
        PlainAccessResource plainAccessResource = new PlainAccessResource();
        plainAccessResource.setRequestCode(17);
        plainPermissionLoader.checkPerm(plainAccessResource, PUBPlainAccessResource);
    }

    @Test
    public void checkPerm() {

        PlainAccessResource plainAccessResource = new PlainAccessResource();
        plainAccessResource.addResourceAndPerm("topicA", Permission.PUB);
        plainPermissionLoader.checkPerm(plainAccessResource, PUBPlainAccessResource);
        plainAccessResource.addResourceAndPerm("topicB", Permission.SUB);
        plainPermissionLoader.checkPerm(plainAccessResource, ANYPlainAccessResource);

        plainAccessResource = new PlainAccessResource();
        plainAccessResource.addResourceAndPerm("topicB", Permission.SUB);
        plainPermissionLoader.checkPerm(plainAccessResource, SUBPlainAccessResource);
        plainAccessResource.addResourceAndPerm("topicA", Permission.PUB);
        plainPermissionLoader.checkPerm(plainAccessResource, ANYPlainAccessResource);

    }
    @Test(expected = AclException.class)
    public void checkErrorPerm() {

        plainAccessResource = new PlainAccessResource();
        plainAccessResource.addResourceAndPerm("topicF", Permission.SUB);
        plainPermissionLoader.checkPerm(plainAccessResource, SUBPlainAccessResource);
    }
    @Test(expected = AclException.class)
    public void accountNullTest() {
        plainAccessConfig.setAccessKey(null);
        plainPermissionLoader.buildPlainAccessResource(plainAccessConfig);
    }

    @Test(expected = AclException.class)
    public void accountThanTest() {
        plainAccessConfig.setAccessKey("123");
        plainPermissionLoader.buildPlainAccessResource(plainAccessConfig);
    }

    @Test(expected = AclException.class)
    public void passWordtNullTest() {
        plainAccessConfig.setAccessKey(null);
        plainPermissionLoader.buildPlainAccessResource(plainAccessConfig);
    }

    @Test(expected = AclException.class)
    public void passWordThanTest() {
        plainAccessConfig.setAccessKey("123");
        plainPermissionLoader.buildPlainAccessResource(plainAccessConfig);
    }

    @Test(expected = AclException.class)
    public void testPlainAclPlugEngineInit() {
        System.setProperty("rocketmq.home.dir", "");
        new PlainPermissionLoader().load();
    }

    @SuppressWarnings("unchecked")
    @Test
    public void cleanAuthenticationInfoTest() throws IllegalAccessException {
        //plainPermissionLoader.addPlainAccessResource(plainAccessResource);
        Map<String, List<PlainAccessResource>> plainAccessResourceMap = (Map<String, List<PlainAccessResource>>) FieldUtils.readDeclaredField(plainPermissionLoader, "plainAccessResourceMap", true);
        Assert.assertFalse(plainAccessResourceMap.isEmpty());

        plainPermissionLoader.clearPermissionInfo();
        plainAccessResourceMap = (Map<String, List<PlainAccessResource>>) FieldUtils.readDeclaredField(plainPermissionLoader, "plainAccessResourceMap", true);
        Assert.assertTrue(plainAccessResourceMap.isEmpty());
    }

    @Test
    public void isWatchStartTest() {

        PlainPermissionLoader plainPermissionLoader = new PlainPermissionLoader();
        Assert.assertTrue(plainPermissionLoader.isWatchStart());
    }


    @Test
    public void testWatch() throws IOException, IllegalAccessException ,InterruptedException{
        System.setProperty("rocketmq.home.dir", "src/test/resources");
        System.setProperty("rocketmq.acl.plain.file", "/conf/plain_acl-test.yml");
        String fileName =System.getProperty("rocketmq.home.dir", "src/test/resources")+System.getProperty("rocketmq.acl.plain.file", "/conf/plain_acl.yml");
        File transport = new File(fileName);
        transport.delete();
        transport.createNewFile();
        FileWriter writer = new FileWriter(transport);
        writer.write("accounts:\r\n");
        writer.write("- accessKey: watchrocketmq\r\n");
        writer.write("  secretKey: 12345678\r\n");
        writer.write("  whiteRemoteAddress: 127.0.0.1\r\n");
        writer.write("  admin: true\r\n");
        writer.flush();
        writer.close();

        PlainPermissionLoader plainPermissionLoader = new PlainPermissionLoader();
        Assert.assertTrue(plainPermissionLoader.isWatchStart());

        {
            Map<String, PlainAccessResource> plainAccessResourceMap = (Map<String, PlainAccessResource>) FieldUtils.readDeclaredField(plainPermissionLoader, "plainAccessResourceMap", true);
            PlainAccessResource accessResource = plainAccessResourceMap.get("watchrocketmq");
            Assert.assertNotNull(accessResource);
            Assert.assertEquals(accessResource.getSecretKey(), "12345678");
            Assert.assertTrue(accessResource.isAdmin());

        }

        writer = new FileWriter(new File(fileName), true);
        writer.write("- accessKey: watchrocketmq1\r\n");
        writer.write("  secretKey: 88888888\r\n");
        writer.write("  whiteRemoteAddress: 127.0.0.1\r\n");
        writer.write("  admin: false\r\n");
        writer.flush();
        writer.close();

        Thread.sleep(1000);
        {
            Map<String, PlainAccessResource> plainAccessResourceMap = (Map<String, PlainAccessResource>) FieldUtils.readDeclaredField(plainPermissionLoader, "plainAccessResourceMap", true);
            PlainAccessResource accessResource = plainAccessResourceMap.get("watchrocketmq1");
            Assert.assertNotNull(accessResource);
            Assert.assertEquals(accessResource.getSecretKey(), "88888888");
            Assert.assertFalse(accessResource.isAdmin());

        }
        transport.delete();
        System.setProperty("rocketmq.home.dir", "src/test/resources");
        System.setProperty("rocketmq.acl.plain.file", "/conf/plain_acl.yml");
    }

    @Test(expected = AclException.class)
    public void initializeTest() {
        System.setProperty("rocketmq.acl.plain.file", "/conf/plain_acl_null.yml");
        new PlainPermissionLoader();

    }

}
