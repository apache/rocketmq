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
package org.apache.rocketmq.acl.common;

import com.alibaba.fastjson.JSONArray;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.rocketmq.acl.plain.PlainAccessResource;
import org.junit.Assert;
import org.junit.Test;

public class PermissionTest {

    @Test
    public void fromStringGetPermissionTest() {
        byte perm = Permission.fromStringGetPermission("PUB");
        Assert.assertEquals(perm, Permission.PUB);

        perm = Permission.fromStringGetPermission("SUB");
        Assert.assertEquals(perm, Permission.SUB);

        perm = Permission.fromStringGetPermission("ANY");
        Assert.assertEquals(perm, Permission.ANY);

        perm = Permission.fromStringGetPermission("PUB|SUB");
        Assert.assertEquals(perm, Permission.ANY);

        perm = Permission.fromStringGetPermission("SUB|PUB");
        Assert.assertEquals(perm, Permission.ANY);

        perm = Permission.fromStringGetPermission("DENY");
        Assert.assertEquals(perm, Permission.DENY);

        perm = Permission.fromStringGetPermission("1");
        Assert.assertEquals(perm, Permission.DENY);

        perm = Permission.fromStringGetPermission(null);
        Assert.assertEquals(perm, Permission.DENY);

    }

    @Test
    public void checkPermissionTest() {
        boolean boo = Permission.checkPermission(Permission.DENY, Permission.DENY);
        Assert.assertFalse(boo);

        boo = Permission.checkPermission(Permission.PUB, Permission.PUB);
        Assert.assertTrue(boo);

        boo = Permission.checkPermission(Permission.SUB, Permission.SUB);
        Assert.assertTrue(boo);

        boo = Permission.checkPermission(Permission.ANY, Permission.ANY);
        Assert.assertFalse(boo);

        boo = Permission.checkPermission(Permission.ANY, Permission.SUB);
        Assert.assertTrue(boo);

        boo = Permission.checkPermission(Permission.ANY, Permission.PUB);
        Assert.assertTrue(boo);

        boo = Permission.checkPermission(Permission.DENY, Permission.ANY);
        Assert.assertFalse(boo);

        boo = Permission.checkPermission(Permission.DENY, Permission.PUB);
        Assert.assertFalse(boo);

        boo = Permission.checkPermission(Permission.DENY, Permission.SUB);
        Assert.assertFalse(boo);

    }

    @Test(expected = AclException.class)
    public void setTopicPermTest() {
        PlainAccessResource plainAccessResource = new PlainAccessResource();
        Map<String, Byte> resourcePermMap = plainAccessResource.getResourcePermMap();

        Permission.setTopicPerm(plainAccessResource, false, null);
        Assert.assertNull(resourcePermMap);

        JSONArray groups = new JSONArray();
        Permission.setTopicPerm(plainAccessResource, false, groups);
        Assert.assertNull(resourcePermMap);

        groups.add("groupA=DENY");
        groups.add("groupB=PUB|SUB");
        groups.add("groupC=PUB");
        Permission.setTopicPerm(plainAccessResource, false, groups);
        resourcePermMap = plainAccessResource.getResourcePermMap();

        byte perm = resourcePermMap.get(PlainAccessResource.getRetryTopic("groupA"));
        Assert.assertEquals(perm, Permission.DENY);

        perm = resourcePermMap.get(PlainAccessResource.getRetryTopic("groupB"));
        Assert.assertEquals(perm, Permission.ANY);

        perm = resourcePermMap.get(PlainAccessResource.getRetryTopic("groupC"));
        Assert.assertEquals(perm, Permission.PUB);

        JSONArray topics = new JSONArray();
        topics.add("topicA=DENY");
        topics.add("topicB=PUB|SUB");
        topics.add("topicC=PUB");

        Permission.setTopicPerm(plainAccessResource, true, topics);

        perm = resourcePermMap.get("topicA");
        Assert.assertEquals(perm, Permission.DENY);

        perm = resourcePermMap.get("topicB");
        Assert.assertEquals(perm, Permission.ANY);

        perm = resourcePermMap.get("topicC");
        Assert.assertEquals(perm, Permission.PUB);

        JSONArray erron = new JSONArray();
        erron.add("");
        Permission.setTopicPerm(plainAccessResource, false, erron);
    }

    @Test
    public void checkAdminCodeTest() {
        Set<Integer> code = new HashSet<>();
        code.add(17);
        code.add(25);
        code.add(215);
        code.add(200);
        code.add(207);

        for (int i = 0; i < 400; i++) {
            boolean boo = Permission.checkAdminCode(i);
            if (boo) {
                Assert.assertTrue(code.contains(i));
            }
        }
    }
}
