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

import com.alibaba.fastjson2.JSONObject;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.remoting.RPCHook;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class AclUtilsTest {

    @Test
    public void testGetAddresses() {
        String address = "1.1.1.{1,2,3,4}";
        String[] addressArray = AclUtils.getAddresses(address, "{1,2,3,4}");
        List<String> newAddressList = new ArrayList<>(Arrays.asList(addressArray));

        List<String> addressList = new ArrayList<>();
        addressList.add("1.1.1.1");
        addressList.add("1.1.1.2");
        addressList.add("1.1.1.3");
        addressList.add("1.1.1.4");
        Assert.assertEquals(newAddressList, addressList);

        // IPv6 test
        String ipv6Address = "1:ac41:9987::bb22:666:{1,2,3,4}";
        String[] ipv6AddressArray = AclUtils.getAddresses(ipv6Address, "{1,2,3,4}");
        List<String> newIPv6AddressList = new ArrayList<>();
        Collections.addAll(newIPv6AddressList, ipv6AddressArray);

        List<String> ipv6AddressList = new ArrayList<>();
        ipv6AddressList.add("1:ac41:9987::bb22:666:1");
        ipv6AddressList.add("1:ac41:9987::bb22:666:2");
        ipv6AddressList.add("1:ac41:9987::bb22:666:3");
        ipv6AddressList.add("1:ac41:9987::bb22:666:4");
        Assert.assertEquals(newIPv6AddressList, ipv6AddressList);
    }

    @Test
    public void testIsScope_StringArray() {
        String address = "12";

        for (int i = 0; i < 6; i++) {
            boolean isScope = AclUtils.isScope(address, 4);
            if (i == 3) {
                Assert.assertTrue(isScope);
            } else {
                Assert.assertFalse(isScope);
            }
            address = address + ".12";
        }
    }

    @Test
    public void testIsScope_Array() {
        String[] address = StringUtils.split("12.12.12.12", ".");
        boolean isScope = AclUtils.isScope(address, 4);
        Assert.assertTrue(isScope);
        isScope = AclUtils.isScope(address, 3);
        Assert.assertTrue(isScope);

        address = StringUtils.split("12.12.1222.1222", ".");
        isScope = AclUtils.isScope(address, 4);
        Assert.assertFalse(isScope);
        isScope = AclUtils.isScope(address, 3);
        Assert.assertFalse(isScope);

        // IPv6 test
        address = StringUtils.split("1050:0000:0000:0000:0005:0600:300c:326b", ":");
        isScope = AclUtils.isIPv6Scope(address, 8);
        Assert.assertTrue(isScope);
        isScope = AclUtils.isIPv6Scope(address, 4);
        Assert.assertTrue(isScope);

        address = StringUtils.split("1050:9876:0000:0000:0005:akkg:300c:326b", ":");
        isScope = AclUtils.isIPv6Scope(address, 8);
        Assert.assertFalse(isScope);
        isScope = AclUtils.isIPv6Scope(address, 4);
        Assert.assertTrue(isScope);

        address = StringUtils.split(AclUtils.expandIP("1050::0005:akkg:300c:326b", 8), ":");
        isScope = AclUtils.isIPv6Scope(address, 8);
        Assert.assertFalse(isScope);
        isScope = AclUtils.isIPv6Scope(address, 4);
        Assert.assertTrue(isScope);
    }

    @Test
    public void testIsScope_String() {
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
    public void testIsScope_Integral() {
        for (int i = 0; i < 256; i++) {
            boolean isScope = AclUtils.isScope(i);
            Assert.assertTrue(isScope);
        }
        boolean isScope = AclUtils.isScope(-1);
        Assert.assertFalse(isScope);
        isScope = AclUtils.isScope(256);
        Assert.assertFalse(isScope);

        // IPv6 test
        int min = Integer.parseInt("0", 16);
        int max = Integer.parseInt("ffff", 16);
        for (int i = min; i < max + 1; i++) {
            isScope = AclUtils.isIPv6Scope(i);
            Assert.assertTrue(isScope);
        }
        isScope = AclUtils.isIPv6Scope(-1);
        Assert.assertFalse(isScope);
        isScope = AclUtils.isIPv6Scope(max + 1);
        Assert.assertFalse(isScope);
    }

    @Test
    public void testIsAsterisk() {
        boolean isAsterisk = AclUtils.isAsterisk("*");
        Assert.assertTrue(isAsterisk);

        isAsterisk = AclUtils.isAsterisk(",");
        Assert.assertFalse(isAsterisk);
    }

    @Test
    public void testIsComma() {
        boolean isColon = AclUtils.isComma(",");
        Assert.assertTrue(isColon);

        isColon = AclUtils.isComma("-");
        Assert.assertFalse(isColon);
    }

    @Test
    public void testIsMinus() {
        boolean isMinus = AclUtils.isMinus("-");
        Assert.assertTrue(isMinus);

        isMinus = AclUtils.isMinus("*");
        Assert.assertFalse(isMinus);
    }

    @Test
    public void testV6ipProcess() {
        String remoteAddr = "5::7:6:1-200:*";
        Assert.assertEquals(AclUtils.v6ipProcess(remoteAddr), "0005:0000:0000:0000:0007:0006");

        remoteAddr = "5::7:6:1-200";
        Assert.assertEquals(AclUtils.v6ipProcess(remoteAddr), "0005:0000:0000:0000:0000:0007:0006");
        remoteAddr = "5::7:6:*";
        Assert.assertEquals(AclUtils.v6ipProcess(remoteAddr), "0005:0000:0000:0000:0000:0007:0006");

        remoteAddr = "5:7:6:*";
        Assert.assertEquals(AclUtils.v6ipProcess(remoteAddr), "0005:0007:0006");
    }

    @Test
    public void testExpandIP() {
        Assert.assertEquals(AclUtils.expandIP("::", 8), "0000:0000:0000:0000:0000:0000:0000:0000");
        Assert.assertEquals(AclUtils.expandIP("::1", 8), "0000:0000:0000:0000:0000:0000:0000:0001");
        Assert.assertEquals(AclUtils.expandIP("3::", 8), "0003:0000:0000:0000:0000:0000:0000:0000");
        Assert.assertEquals(AclUtils.expandIP("2::2", 8), "0002:0000:0000:0000:0000:0000:0000:0002");
        Assert.assertEquals(AclUtils.expandIP("4::aac4:92", 8), "0004:0000:0000:0000:0000:0000:AAC4:0092");
        Assert.assertEquals(AclUtils.expandIP("ab23:56:901a::cc6:765:bb:9011", 8), "AB23:0056:901A:0000:0CC6:0765:00BB:9011");
        Assert.assertEquals(AclUtils.expandIP("ab23:56:901a:1:cc6:765:bb:9011", 8), "AB23:0056:901A:0001:0CC6:0765:00BB:9011");
        Assert.assertEquals(AclUtils.expandIP("5::7:6", 6), "0005:0000:0000:0000:0007:0006");
    }

    private static String randomTmpFile() {
        String tmpFileName = System.getProperty("java.io.tmpdir");
        // https://rationalpi.wordpress.com/2007/01/26/javaiotmpdir-inconsitency/
        if (!tmpFileName.endsWith(File.separator)) {
            tmpFileName += File.separator;
        }
        tmpFileName += UUID.randomUUID() + ".yml";
        return tmpFileName;
    }

    @Test
    public void getYamlDataIgnoreFileNotFoundExceptionTest() {

        JSONObject yamlDataObject = AclUtils.getYamlDataObject("plain_acl.yml", JSONObject.class);
        Assert.assertNull(yamlDataObject);
    }

    @Test
    public void getAclRPCHookTest() throws IOException {
        try (InputStream is = AclUtilsTest.class.getClassLoader().getResourceAsStream("conf/plain_acl_incomplete.yml")) {
            RPCHook incompleteContRPCHook = AclUtils.getAclRPCHook(is);
            Assert.assertNull(incompleteContRPCHook);
        }
    }

    @Test
    public void testGetAclRPCHookByFileName() {
        // Skip this test if running in Bazel, as the resource path is a path inside the JAR.
        Assume.assumeTrue(System.getProperty("build.bazel") == null);
        RPCHook actual = AclUtils.getAclRPCHook(Objects.requireNonNull(AclUtilsTest.class.getResource("/acl_hook/plain_acl.yml")).getPath());
        assertNotNull(actual);
        assertTrue(actual instanceof AclClientRPCHook);
        assertAclClientRPCHook((AclClientRPCHook) actual);
    }

    @Test
    public void testGetAclRPCHookByInputStream() {
        RPCHook actual = AclUtils.getAclRPCHook(Objects.requireNonNull(AclUtilsTest.class.getResourceAsStream("/acl_hook/plain_acl.yml")));
        assertNotNull(actual);
        assertTrue(actual instanceof AclClientRPCHook);
        assertAclClientRPCHook((AclClientRPCHook) actual);
    }

    private void assertAclClientRPCHook(final AclClientRPCHook actual) {
        assertEquals("rocketmq2", actual.getSessionCredentials().getAccessKey());
        assertEquals("12345678", actual.getSessionCredentials().getSecretKey());
    }
}
