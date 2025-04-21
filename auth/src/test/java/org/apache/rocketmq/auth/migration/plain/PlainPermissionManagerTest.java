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
package org.apache.rocketmq.auth.migration.plain;

import org.apache.rocketmq.acl.common.Permission;
import org.apache.rocketmq.auth.migration.v1.PlainAccessResource;
import org.apache.rocketmq.auth.migration.v1.PlainPermissionManager;
import org.assertj.core.api.Assertions;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class PlainPermissionManagerTest {

    PlainPermissionManager plainPermissionManager;
    PlainAccessResource    pubPlainAccessResource;
    PlainAccessResource    subPlainAccessResource;
    PlainAccessResource    anyPlainAccessResource;
    PlainAccessResource    denyPlainAccessResource;
    Set<Integer>           adminCode = new HashSet<>();

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
    public void getAllAclFilesTest() {
        final List<String> notExistList = plainPermissionManager.getAllAclFiles("aa/bb");
        Assertions.assertThat(notExistList).isEmpty();
        final List<String> files = plainPermissionManager.getAllAclFiles(confHome.getAbsolutePath());
        Assertions.assertThat(files).isNotEmpty();
    }

    @Test
    public void loadTest() {
        plainPermissionManager.load();
    }
}
