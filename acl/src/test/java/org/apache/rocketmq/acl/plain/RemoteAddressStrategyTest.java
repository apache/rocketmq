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

import org.apache.rocketmq.acl.common.AclException;
import org.junit.Assert;
import org.junit.Test;

public class RemoteAddressStrategyTest {

    RemoteAddressStrategyFactory remoteAddressStrategyFactory = new RemoteAddressStrategyFactory();

    @Test
    public void netaddressStrategyFactoryExceptionTest() {
        PlainAccessResource plainAccessResource = new PlainAccessResource();
        remoteAddressStrategyFactory.getRemoteAddressStrategy(plainAccessResource);
        Assert.assertEquals(remoteAddressStrategyFactory.getRemoteAddressStrategy(plainAccessResource).getClass(),
            RemoteAddressStrategyFactory.BlankRemoteAddressStrategy.class);
    }

    @Test
    public void netaddressStrategyFactoryTest() {
        PlainAccessResource plainAccessResource = new PlainAccessResource();

        plainAccessResource.setWhiteRemoteAddress("*");
        RemoteAddressStrategy remoteAddressStrategy = remoteAddressStrategyFactory.getRemoteAddressStrategy(plainAccessResource);
        Assert.assertEquals(remoteAddressStrategy, RemoteAddressStrategyFactory.NULL_NET_ADDRESS_STRATEGY);

        plainAccessResource.setWhiteRemoteAddress("127.0.0.1");
        remoteAddressStrategy = remoteAddressStrategyFactory.getRemoteAddressStrategy(plainAccessResource);
        Assert.assertEquals(remoteAddressStrategy.getClass(), RemoteAddressStrategyFactory.OneRemoteAddressStrategy.class);

        plainAccessResource.setWhiteRemoteAddress("127.0.0.1,127.0.0.2,127.0.0.3");
        remoteAddressStrategy = remoteAddressStrategyFactory.getRemoteAddressStrategy(plainAccessResource);
        Assert.assertEquals(remoteAddressStrategy.getClass(), RemoteAddressStrategyFactory.MultipleRemoteAddressStrategy.class);

        plainAccessResource.setWhiteRemoteAddress("127.0.0.{1,2,3}");
        remoteAddressStrategy = remoteAddressStrategyFactory.getRemoteAddressStrategy(plainAccessResource);
        Assert.assertEquals(remoteAddressStrategy.getClass(), RemoteAddressStrategyFactory.MultipleRemoteAddressStrategy.class);

        plainAccessResource.setWhiteRemoteAddress("127.0.0.1-200");
        remoteAddressStrategy = remoteAddressStrategyFactory.getRemoteAddressStrategy(plainAccessResource);
        Assert.assertEquals(remoteAddressStrategy.getClass(), RemoteAddressStrategyFactory.RangeRemoteAddressStrategy.class);

        plainAccessResource.setWhiteRemoteAddress("127.0.0.*");
        remoteAddressStrategy = remoteAddressStrategyFactory.getRemoteAddressStrategy(plainAccessResource);
        Assert.assertEquals(remoteAddressStrategy.getClass(), RemoteAddressStrategyFactory.RangeRemoteAddressStrategy.class);

        plainAccessResource.setWhiteRemoteAddress("127.0.1-20.*");
        remoteAddressStrategy = remoteAddressStrategyFactory.getRemoteAddressStrategy(plainAccessResource);
        Assert.assertEquals(remoteAddressStrategy.getClass(), RemoteAddressStrategyFactory.RangeRemoteAddressStrategy.class);

        plainAccessResource.setWhiteRemoteAddress("");
        remoteAddressStrategy = remoteAddressStrategyFactory.getRemoteAddressStrategy(plainAccessResource);
        Assert.assertEquals(remoteAddressStrategy.getClass(), RemoteAddressStrategyFactory.BlankRemoteAddressStrategy.class);
    }

    @Test(expected = AclException.class)
    public void verifyTest() {
        PlainAccessResource plainAccessResource = new PlainAccessResource();
        plainAccessResource.setWhiteRemoteAddress("127.0.0.1");
        remoteAddressStrategyFactory.getRemoteAddressStrategy(plainAccessResource);
        plainAccessResource.setWhiteRemoteAddress("256.0.0.1");
        remoteAddressStrategyFactory.getRemoteAddressStrategy(plainAccessResource);
    }

    @Test
    public void nullNetaddressStrategyTest() {
        boolean isMatch = RemoteAddressStrategyFactory.NULL_NET_ADDRESS_STRATEGY.match(new PlainAccessResource());
        Assert.assertTrue(isMatch);
    }

    @Test
    public void blankNetaddressStrategyTest() {
        boolean isMatch = RemoteAddressStrategyFactory.BLANK_NET_ADDRESS_STRATEGY.match(new PlainAccessResource());
        Assert.assertFalse(isMatch);
    }

    public void oneNetaddressStrategyTest() {
        PlainAccessResource plainAccessResource = new PlainAccessResource();
        plainAccessResource.setWhiteRemoteAddress("127.0.0.1");
        RemoteAddressStrategy remoteAddressStrategy = remoteAddressStrategyFactory.getRemoteAddressStrategy(plainAccessResource);
        plainAccessResource.setWhiteRemoteAddress("");
        boolean match = remoteAddressStrategy.match(plainAccessResource);
        Assert.assertFalse(match);

        plainAccessResource.setWhiteRemoteAddress("127.0.0.2");
        match = remoteAddressStrategy.match(plainAccessResource);
        Assert.assertFalse(match);

        plainAccessResource.setWhiteRemoteAddress("127.0.0.1");
        match = remoteAddressStrategy.match(plainAccessResource);
        Assert.assertTrue(match);
    }

    @Test
    public void multipleNetaddressStrategyTest() {
        PlainAccessResource plainAccessResource = new PlainAccessResource();
        plainAccessResource.setWhiteRemoteAddress("127.0.0.1,127.0.0.2,127.0.0.3");
        RemoteAddressStrategy remoteAddressStrategy = remoteAddressStrategyFactory.getRemoteAddressStrategy(plainAccessResource);
        multipleNetaddressStrategyTest(remoteAddressStrategy);

        plainAccessResource.setWhiteRemoteAddress("127.0.0.{1,2,3}");
        remoteAddressStrategy = remoteAddressStrategyFactory.getRemoteAddressStrategy(plainAccessResource);
        multipleNetaddressStrategyTest(remoteAddressStrategy);

    }

    @Test(expected = AclException.class)
    public void multipleNetaddressStrategyExceptionTest() {
        PlainAccessResource plainAccessResource = new PlainAccessResource();
        plainAccessResource.setWhiteRemoteAddress("127.0.0.1,2,3}");
        remoteAddressStrategyFactory.getRemoteAddressStrategy(plainAccessResource);
    }

    private void multipleNetaddressStrategyTest(RemoteAddressStrategy remoteAddressStrategy) {
        PlainAccessResource plainAccessResource = new PlainAccessResource();
        plainAccessResource.setWhiteRemoteAddress("127.0.0.1");
        boolean match = remoteAddressStrategy.match(plainAccessResource);
        Assert.assertTrue(match);

        plainAccessResource.setWhiteRemoteAddress("127.0.0.2");
        match = remoteAddressStrategy.match(plainAccessResource);
        Assert.assertTrue(match);

        plainAccessResource.setWhiteRemoteAddress("127.0.0.3");
        match = remoteAddressStrategy.match(plainAccessResource);
        Assert.assertTrue(match);

        plainAccessResource.setWhiteRemoteAddress("127.0.0.4");
        match = remoteAddressStrategy.match(plainAccessResource);
        Assert.assertFalse(match);

        plainAccessResource.setWhiteRemoteAddress("127.0.0.0");
        match = remoteAddressStrategy.match(plainAccessResource);
        Assert.assertFalse(match);

    }

    @Test
    public void rangeNetaddressStrategyTest() {
        String head = "127.0.0.";
        PlainAccessResource plainAccessResource = new PlainAccessResource();
        plainAccessResource.setWhiteRemoteAddress("127.0.0.1-200");
        RemoteAddressStrategy remoteAddressStrategy = remoteAddressStrategyFactory.getRemoteAddressStrategy(plainAccessResource);
        rangeNetaddressStrategyTest(remoteAddressStrategy, head, 1, 200, true);
        plainAccessResource.setWhiteRemoteAddress("127.0.0.*");
        remoteAddressStrategy = remoteAddressStrategyFactory.getRemoteAddressStrategy(plainAccessResource);
        rangeNetaddressStrategyTest(remoteAddressStrategy, head, 0, 255, true);

        plainAccessResource.setWhiteRemoteAddress("127.0.1-200.*");
        remoteAddressStrategy = remoteAddressStrategyFactory.getRemoteAddressStrategy(plainAccessResource);
        rangeNetaddressStrategyThirdlyTest(remoteAddressStrategy, head, 1, 200);
    }

    private void rangeNetaddressStrategyTest(RemoteAddressStrategy remoteAddressStrategy, String head, int start,
        int end,
        boolean isFalse) {
        PlainAccessResource plainAccessResource = new PlainAccessResource();
        for (int i = -10; i < 300; i++) {
            plainAccessResource.setWhiteRemoteAddress(head + i);
            boolean match = remoteAddressStrategy.match(plainAccessResource);
            if (isFalse && i >= start && i <= end) {
                Assert.assertTrue(match);
                continue;
            }
            Assert.assertFalse(match);

        }
    }

    private void rangeNetaddressStrategyThirdlyTest(RemoteAddressStrategy remoteAddressStrategy, String head, int start,
        int end) {
        String newHead;
        for (int i = -10; i < 300; i++) {
            newHead = head + i;
            if (i >= start && i <= end) {
                rangeNetaddressStrategyTest(remoteAddressStrategy, newHead, 0, 255, false);
            }
        }
    }

    @Test(expected = AclException.class)
    public void rangeNetaddressStrategyExceptionStartGreaterEndTest() {
        rangeNetaddressStrategyExceptionTest("127.0.0.2-1");
    }

    @Test(expected = AclException.class)
    public void rangeNetaddressStrategyExceptionScopeTest() {
        rangeNetaddressStrategyExceptionTest("127.0.0.-1-200");
    }

    @Test(expected = AclException.class)
    public void rangeNetaddressStrategyExceptionScopeTwoTest() {
        rangeNetaddressStrategyExceptionTest("127.0.0.0-256");
    }

    private void rangeNetaddressStrategyExceptionTest(String netaddress) {
        PlainAccessResource plainAccessResource = new PlainAccessResource();
        plainAccessResource.setWhiteRemoteAddress(netaddress);
        remoteAddressStrategyFactory.getRemoteAddressStrategy(plainAccessResource);
    }

}
