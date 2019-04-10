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

import com.alibaba.fastjson.JSONObject;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.remoting.RPCHook;
import org.junit.Assert;
import org.junit.Test;

public class AclUtilsTest {

    @Test
    public void getAddreeStrArray() {
        String address = "1.1.1.{1,2,3,4}";
        String[] addressArray = AclUtils.getAddreeStrArray(address, "{1,2,3,4}");
        List<String> newAddressList = new ArrayList<>();
        for (String a : addressArray) {
            newAddressList.add(a);
        }

        List<String> addressList = new ArrayList<>();
        addressList.add("1.1.1.1");
        addressList.add("1.1.1.2");
        addressList.add("1.1.1.3");
        addressList.add("1.1.1.4");
        Assert.assertEquals(newAddressList, addressList);
    }

    @Test
    public void isScopeStringArray() {
        String adderss = "12";

        for (int i = 0; i < 6; i++) {
            boolean isScope = AclUtils.isScope(adderss, 4);
            if (i == 3) {
                Assert.assertTrue(isScope);
            } else {
                Assert.assertFalse(isScope);
            }
            adderss = adderss + ".12";
        }
    }

    @Test
    public void isScopeArray() {
        String[] adderss = StringUtils.split("12.12.12.12", ".");
        boolean isScope = AclUtils.isScope(adderss, 4);
        Assert.assertTrue(isScope);
        isScope = AclUtils.isScope(adderss, 3);
        Assert.assertTrue(isScope);

        adderss = StringUtils.split("12.12.1222.1222", ".");
        isScope = AclUtils.isScope(adderss, 4);
        Assert.assertFalse(isScope);
        isScope = AclUtils.isScope(adderss, 3);
        Assert.assertFalse(isScope);

    }

    @Test
    public void isScopeStringTest() {
        for (int i = 0; i < 256; i++) {
            boolean isScope = AclUtils.isScope(i + "");
            Assert.assertTrue(isScope);
        }
        boolean isScope = AclUtils.isScope("-1");
        Assert.assertFalse(isScope);
        isScope = AclUtils.isScope("256");
        Assert.assertFalse(isScope);
    }

    @Test
    public void isScopeTest() {
        for (int i = 0; i < 256; i++) {
            boolean isScope = AclUtils.isScope(i);
            Assert.assertTrue(isScope);
        }
        boolean isScope = AclUtils.isScope(-1);
        Assert.assertFalse(isScope);
        isScope = AclUtils.isScope(256);
        Assert.assertFalse(isScope);

    }

    @Test
    public void isAsteriskTest() {
        boolean isAsterisk = AclUtils.isAsterisk("*");
        Assert.assertTrue(isAsterisk);

        isAsterisk = AclUtils.isAsterisk(",");
        Assert.assertFalse(isAsterisk);
    }

    @Test
    public void isColonTest() {
        boolean isColon = AclUtils.isColon(",");
        Assert.assertTrue(isColon);

        isColon = AclUtils.isColon("-");
        Assert.assertFalse(isColon);
    }

    @Test
    public void isMinusTest() {
        boolean isMinus = AclUtils.isMinus("-");
        Assert.assertTrue(isMinus);

        isMinus = AclUtils.isMinus("*");
        Assert.assertFalse(isMinus);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void getYamlDataObjectTest() {

        Map<String, Object> map = AclUtils.getYamlDataObject("src/test/resources/conf/plain_acl.yml", Map.class);
        Assert.assertFalse(map.isEmpty());
    }

    @Test
    public void getYamlDataIgnoreFileNotFoundExceptionTest() {

        JSONObject yamlDataObject = AclUtils.getYamlDataObject("plain_acl.yml", JSONObject.class);
        Assert.assertTrue(yamlDataObject == null);
    }

    @Test(expected = Exception.class)
    public void getYamlDataExceptionTest() {

        AclUtils.getYamlDataObject("src/test/resources/conf/plain_acl_format_error.yml", Map.class);
    }

    @Test
    public void getAclRPCHookTest() {

        RPCHook errorContRPCHook = AclUtils.getAclRPCHook("src/test/resources/conf/plain_acl_format_error.yml");
        Assert.assertNull(errorContRPCHook);

        RPCHook noFileRPCHook = AclUtils.getAclRPCHook("src/test/resources/plain_acl_format_error1.yml");
        Assert.assertNull(noFileRPCHook);

        RPCHook emptyContRPCHook = AclUtils.getAclRPCHook("src/test/resources/conf/plain_acl_null.yml");
        Assert.assertNull(emptyContRPCHook);

        RPCHook incompleteContRPCHook = AclUtils.getAclRPCHook("src/test/resources/conf/plain_acl_incomplete.yml");
        Assert.assertNull(incompleteContRPCHook);
    }


}
