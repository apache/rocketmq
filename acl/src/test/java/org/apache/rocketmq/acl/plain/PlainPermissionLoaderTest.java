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

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.rocketmq.acl.common.AclException;
import org.apache.rocketmq.acl.common.AclUtils;
import org.apache.rocketmq.acl.common.Permission;
import org.apache.rocketmq.common.MixAll;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

@RunWith(PowerMockRunner.class)
@PrepareForTest({AclUtils.class})
public class PlainPermissionLoaderTest {

    PlainPermissionLoader plainPermissionLoader;
    PlainAccessResource PUBPlainAccessResource;
    PlainAccessResource SUBPlainAccessResource;
    PlainAccessResource ANYPlainAccessResource;
    PlainAccessResource DENYPlainAccessResource;
    PlainAccessResource plainAccessResource = new PlainAccessResource();
    PlainAccessResource plainAccessResourceTwo = new PlainAccessResource();
    Set<Integer> adminCode = new HashSet<>();
    private String fileName = System.getProperty("romcketmq.acl.plain.fileName", "/conf/transport.yml");
    private Map<String/** account **/
        , List<PlainAccessResource>> plainAccessResourceMap;
    private List<RemoteAddressStrategy> globalWhiteRemoteAddressStrategy;

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

        System.setProperty("java.version", "1.6.11");
        System.setProperty("rocketmq.home.dir", "src/test/resources");
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

    @SuppressWarnings("unchecked")
    private void getField(PlainPermissionLoader plainPermissionLoader) {
        try {
            this.globalWhiteRemoteAddressStrategy = (List<RemoteAddressStrategy>) FieldUtils.readDeclaredField(plainPermissionLoader, "globalWhiteRemoteAddressStrategy", true);
            this.plainAccessResourceMap = (Map<String/** account **/, List<PlainAccessResource>>) FieldUtils.readDeclaredField(plainPermissionLoader, "plainAccessResourceMap", true);
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }
    }

    @Test(expected = AclException.class)
    public void initializeTest() {
        System.setProperty("romcketmq.acl.plain.fileName", "/conf/transport-null.yml");
        new PlainPermissionLoader();

    }

    @Test
    public void initializeIngetYamlDataObject() {
        String fileHome = System.getProperty(MixAll.ROCKETMQ_HOME_PROPERTY, System.getenv(MixAll.ROCKETMQ_HOME_ENV));
        PowerMockito.mockStatic(AclUtils.class);
        JSONObject json = new JSONObject();
        json.put("", "");
        PowerMockito.when(AclUtils.getYamlDataObject(fileHome + "/conf/transport.yml", JSONObject.class)).thenReturn(json);
        PlainPermissionLoader plainPermissionLoader = new PlainPermissionLoader();
        getField(plainPermissionLoader);
        Assert.assertTrue(globalWhiteRemoteAddressStrategy.isEmpty());
        Assert.assertTrue(plainAccessResourceMap.isEmpty());
    }

    @Test
    public void getPlainAccessResourceTest() {
        PlainAccessResource plainAccessResource = new PlainAccessResource();
        JSONObject account = new JSONObject();
        account.put("accessKey", "RocketMQ");
        plainAccessResource = plainPermissionLoader.getPlainAccessResource(account);
        Assert.assertEquals(plainAccessResource.getAccessKey(), "RocketMQ");

        account.put("secretKey", "12345678");
        plainAccessResource = plainPermissionLoader.getPlainAccessResource(account);
        Assert.assertEquals(plainAccessResource.getSecretKey(), "12345678");

        account.put("whiteRemoteAddress", "127.0.0.1");
        plainAccessResource = plainPermissionLoader.getPlainAccessResource(account);
        Assert.assertEquals(plainAccessResource.getWhiteRemoteAddress(), "127.0.0.1");

        account.put("admin", true);
        plainAccessResource = plainPermissionLoader.getPlainAccessResource(account);
        Assert.assertEquals(plainAccessResource.isAdmin(), true);

        account.put("defaultGroupPerm", "ANY");
        plainAccessResource = plainPermissionLoader.getPlainAccessResource(account);
        Assert.assertEquals(plainAccessResource.getDefaultGroupPerm(), Permission.ANY);

        account.put("defaultTopicPerm", "ANY");
        plainAccessResource = plainPermissionLoader.getPlainAccessResource(account);
        Assert.assertEquals(plainAccessResource.getDefaultTopicPerm(), Permission.ANY);

        JSONArray groups = new JSONArray();
        groups.add("groupA=DENY");
        groups.add("groupB=PUB|SUB");
        groups.add("groupC=PUB");
        account.put("groups", groups);
        plainAccessResource = plainPermissionLoader.getPlainAccessResource(account);
        Map<String, Byte> resourcePermMap = plainAccessResource.getResourcePermMap();
        Assert.assertEquals(resourcePermMap.size(), 3);

        Assert.assertEquals(resourcePermMap.get("groupA").byteValue(), Permission.DENY);
        Assert.assertEquals(resourcePermMap.get("groupB").byteValue(), Permission.ANY);
        Assert.assertEquals(resourcePermMap.get("groupC").byteValue(), Permission.PUB);

        JSONArray topics = new JSONArray();
        topics.add("topicA=DENY");
        topics.add("topicB=PUB|SUB");
        topics.add("topicC=PUB");
        account.put("topics", topics);
        plainAccessResource = plainPermissionLoader.getPlainAccessResource(account);
        resourcePermMap = plainAccessResource.getResourcePermMap();
        Assert.assertEquals(resourcePermMap.size(), 3);

        Assert.assertEquals(resourcePermMap.get("topicA").byteValue(), Permission.DENY);
        Assert.assertEquals(resourcePermMap.get("topicB").byteValue(), Permission.ANY);
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
        plainAccessResource.addResourceAndPerm("pub", Permission.PUB);
        plainPermissionLoader.checkPerm(PUBPlainAccessResource, plainAccessResource);
        plainAccessResource.addResourceAndPerm("sub", Permission.SUB);
        plainPermissionLoader.checkPerm(ANYPlainAccessResource, plainAccessResource);

        plainAccessResource = new PlainAccessResource();
        plainAccessResource.addResourceAndPerm("sub", Permission.SUB);
        plainPermissionLoader.checkPerm(SUBPlainAccessResource, plainAccessResource);
        plainAccessResource.addResourceAndPerm("pub", Permission.PUB);
        plainPermissionLoader.checkPerm(ANYPlainAccessResource, plainAccessResource);

    }

    @Test(expected = AclException.class)
    public void accountNullTest() {
        plainAccessResource.setAccessKey(null);
        plainPermissionLoader.setPlainAccessResource(plainAccessResource);
    }

    @Test(expected = AclException.class)
    public void accountThanTest() {
        plainAccessResource.setAccessKey("123");
        plainPermissionLoader.setPlainAccessResource(plainAccessResource);
    }

    @Test(expected = AclException.class)
    public void passWordtNullTest() {
        plainAccessResource.setAccessKey(null);
        plainPermissionLoader.setPlainAccessResource(plainAccessResource);
    }

    @Test(expected = AclException.class)
    public void passWordThanTest() {
        plainAccessResource.setAccessKey("123");
        plainPermissionLoader.setPlainAccessResource(plainAccessResource);
    }

    @Test(expected = AclException.class)
    public void testPlainAclPlugEngineInit() {
        System.setProperty("rocketmq.home.dir", "");
        new PlainPermissionLoader().initialize();
    }

    @Test
    public void cleanAuthenticationInfoTest() {
        plainPermissionLoader.setPlainAccessResource(plainAccessResource);
        plainAccessResource.setRequestCode(202);
        plainPermissionLoader.eachCheckPlainAccessResource(plainAccessResource);
        plainPermissionLoader.cleanAuthenticationInfo();
        plainPermissionLoader.eachCheckPlainAccessResource(plainAccessResource);
    }

    @Test
    public void isWatchStartTest() {
        PlainPermissionLoader plainPermissionLoader = new PlainPermissionLoader();
        Assert.assertTrue(plainPermissionLoader.isWatchStart());
        System.setProperty("java.version", "1.6.11");
        plainPermissionLoader = new PlainPermissionLoader();
        Assert.assertFalse(plainPermissionLoader.isWatchStart());
    }

    @Test
    public void watchTest() throws IOException {
        System.setProperty("rocketmq.home.dir", "src/test/resources/watch");
        File file = new File("src/test/resources/watch/conf");
        file.mkdirs();
        File transport = new File("src/test/resources/watch/conf/transport.yml");
        transport.createNewFile();

        FileWriter writer = new FileWriter(transport);
        writer.write("list:\r\n");
        writer.write("- account: rokcetmq\r\n");
        writer.write("  password: aliyun11\r\n");
        writer.write("  netaddress: 127.0.0.1\r\n");
        writer.flush();
        writer.close();
        PlainPermissionLoader plainPermissionLoader = new PlainPermissionLoader();
        plainAccessResource.setRequestCode(203);
        plainPermissionLoader.eachCheckPlainAccessResource(plainAccessResource);

        writer = new FileWriter(new File("src/test/resources/watch/conf/transport.yml"), true);
        writer.write("- account: rokcet1\r\n");
        writer.write("  password: aliyun1\r\n");
        writer.write("  netaddress: 127.0.0.1\r\n");
        writer.flush();
        writer.close();
        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        plainAccessResourceTwo.setRequestCode(203);
        plainPermissionLoader.eachCheckPlainAccessResource(plainAccessResourceTwo);

        transport.delete();
        file.delete();
        file = new File("src/test/resources/watch");
        file.delete();

    }

}
