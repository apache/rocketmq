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

import org.junit.Assert;
import org.junit.Test;

public class NetaddressStrategyTest {

    NetaddressStrategyFactory netaddressStrategyFactory = new NetaddressStrategyFactory();

    @Test
    public void NetaddressStrategyFactoryTest() {
        PlainAccessResource plainAccessResource = new PlainAccessResource();
        NetaddressStrategy netaddressStrategy = netaddressStrategyFactory.getNetaddressStrategy(plainAccessResource);
        Assert.assertEquals(netaddressStrategy, NetaddressStrategyFactory.NULL_NET_ADDRESS_STRATEGY);

        plainAccessResource.setRemoteAddr("*");
        netaddressStrategy = netaddressStrategyFactory.getNetaddressStrategy(plainAccessResource);
        Assert.assertEquals(netaddressStrategy, NetaddressStrategyFactory.NULL_NET_ADDRESS_STRATEGY);

        plainAccessResource.setRemoteAddr("127.0.0.1");
        netaddressStrategy = netaddressStrategyFactory.getNetaddressStrategy(plainAccessResource);
        Assert.assertEquals(netaddressStrategy.getClass(), NetaddressStrategyFactory.OneNetaddressStrategy.class);

        plainAccessResource.setRemoteAddr("127.0.0.1,127.0.0.2,127.0.0.3");
        netaddressStrategy = netaddressStrategyFactory.getNetaddressStrategy(plainAccessResource);
        Assert.assertEquals(netaddressStrategy.getClass(), NetaddressStrategyFactory.MultipleNetaddressStrategy.class);

        plainAccessResource.setRemoteAddr("127.0.0.{1,2,3}");
        netaddressStrategy = netaddressStrategyFactory.getNetaddressStrategy(plainAccessResource);
        Assert.assertEquals(netaddressStrategy.getClass(), NetaddressStrategyFactory.MultipleNetaddressStrategy.class);

        plainAccessResource.setRemoteAddr("127.0.0.1-200");
        netaddressStrategy = netaddressStrategyFactory.getNetaddressStrategy(plainAccessResource);
        Assert.assertEquals(netaddressStrategy.getClass(), NetaddressStrategyFactory.RangeNetaddressStrategy.class);

        plainAccessResource.setRemoteAddr("127.0.0.*");
        netaddressStrategy = netaddressStrategyFactory.getNetaddressStrategy(plainAccessResource);
        Assert.assertEquals(netaddressStrategy.getClass(), NetaddressStrategyFactory.RangeNetaddressStrategy.class);

        plainAccessResource.setRemoteAddr("127.0.1-20.*");
        netaddressStrategy = netaddressStrategyFactory.getNetaddressStrategy(plainAccessResource);
        Assert.assertEquals(netaddressStrategy.getClass(), NetaddressStrategyFactory.RangeNetaddressStrategy.class);
    }

    @Test(expected = AclPlugRuntimeException.class)
    public void verifyTest() {
        PlainAccessResource plainAccessResource = new PlainAccessResource();
        plainAccessResource.setRemoteAddr("127.0.0.1");
        netaddressStrategyFactory.getNetaddressStrategy(plainAccessResource);
        plainAccessResource.setRemoteAddr("256.0.0.1");
        netaddressStrategyFactory.getNetaddressStrategy(plainAccessResource);
    }

    @Test
    public void nullNetaddressStrategyTest() {
        boolean isMatch = NetaddressStrategyFactory.NULL_NET_ADDRESS_STRATEGY.match(new PlainAccessResource());
        Assert.assertTrue(isMatch);
    }

    public void oneNetaddressStrategyTest() {
        PlainAccessResource plainAccessResource = new PlainAccessResource();
        plainAccessResource.setRemoteAddr("127.0.0.1");
        NetaddressStrategy netaddressStrategy = netaddressStrategyFactory.getNetaddressStrategy(plainAccessResource);
        plainAccessResource.setRemoteAddr("");
        boolean match = netaddressStrategy.match(plainAccessResource);
        Assert.assertFalse(match);

        plainAccessResource.setRemoteAddr("127.0.0.2");
        match = netaddressStrategy.match(plainAccessResource);
        Assert.assertFalse(match);

        plainAccessResource.setRemoteAddr("127.0.0.1");
        match = netaddressStrategy.match(plainAccessResource);
        Assert.assertTrue(match);
    }

    @Test
    public void multipleNetaddressStrategyTest() {
        PlainAccessResource plainAccessResource = new PlainAccessResource();
        plainAccessResource.setRemoteAddr("127.0.0.1,127.0.0.2,127.0.0.3");
        NetaddressStrategy netaddressStrategy = netaddressStrategyFactory.getNetaddressStrategy(plainAccessResource);
        multipleNetaddressStrategyTest(netaddressStrategy);

        plainAccessResource.setRemoteAddr("127.0.0.{1,2,3}");
        netaddressStrategy = netaddressStrategyFactory.getNetaddressStrategy(plainAccessResource);
        multipleNetaddressStrategyTest(netaddressStrategy);

    }

    @Test(expected = AclPlugRuntimeException.class)
    public void multipleNetaddressStrategyExceptionTest() {
        PlainAccessResource plainAccessResource = new PlainAccessResource();
        plainAccessResource.setRemoteAddr("127.0.0.1,2,3}");
        netaddressStrategyFactory.getNetaddressStrategy(plainAccessResource);
    }

    private void multipleNetaddressStrategyTest(NetaddressStrategy netaddressStrategy) {
        PlainAccessResource plainAccessResource = new PlainAccessResource();
        plainAccessResource.setRemoteAddr("127.0.0.1");
        boolean match = netaddressStrategy.match(plainAccessResource);
        Assert.assertTrue(match);

        plainAccessResource.setRemoteAddr("127.0.0.2");
        match = netaddressStrategy.match(plainAccessResource);
        Assert.assertTrue(match);

        plainAccessResource.setRemoteAddr("127.0.0.3");
        match = netaddressStrategy.match(plainAccessResource);
        Assert.assertTrue(match);

        plainAccessResource.setRemoteAddr("127.0.0.4");
        match = netaddressStrategy.match(plainAccessResource);
        Assert.assertFalse(match);

        plainAccessResource.setRemoteAddr("127.0.0.0");
        match = netaddressStrategy.match(plainAccessResource);
        Assert.assertFalse(match);

    }

    @Test
    public void rangeNetaddressStrategyTest() {
        String head = "127.0.0.";
        PlainAccessResource plainAccessResource = new PlainAccessResource();
        plainAccessResource.setRemoteAddr("127.0.0.1-200");
        NetaddressStrategy netaddressStrategy = netaddressStrategyFactory.getNetaddressStrategy(plainAccessResource);
        rangeNetaddressStrategyTest(netaddressStrategy, head, 1, 200, true);
        plainAccessResource.setRemoteAddr("127.0.0.*");
        netaddressStrategy = netaddressStrategyFactory.getNetaddressStrategy(plainAccessResource);
        rangeNetaddressStrategyTest(netaddressStrategy, head, 0, 255, true);

        plainAccessResource.setRemoteAddr("127.0.1-200.*");
        netaddressStrategy = netaddressStrategyFactory.getNetaddressStrategy(plainAccessResource);
        rangeNetaddressStrategyThirdlyTest(netaddressStrategy, head, 1, 200);
    }

    private void rangeNetaddressStrategyTest(NetaddressStrategy netaddressStrategy, String head, int start, int end,
        boolean isFalse) {
        PlainAccessResource plainAccessResource = new PlainAccessResource();
        for (int i = -10; i < 300; i++) {
            plainAccessResource.setRemoteAddr(head + i);
            boolean match = netaddressStrategy.match(plainAccessResource);
            if (isFalse && i >= start && i <= end) {
                Assert.assertTrue(match);
                continue;
            }
            Assert.assertFalse(match);

        }
    }

    private void rangeNetaddressStrategyThirdlyTest(NetaddressStrategy netaddressStrategy, String head, int start,
        int end) {
        String newHead;
        for (int i = -10; i < 300; i++) {
            newHead = head + i;
            if (i >= start && i <= end) {
                rangeNetaddressStrategyTest(netaddressStrategy, newHead, 0, 255, false);
            }
        }
    }

    @Test(expected = AclPlugRuntimeException.class)
    public void rangeNetaddressStrategyExceptionStartGreaterEndTest() {
        rangeNetaddressStrategyExceptionTest("127.0.0.2-1");
    }

    @Test(expected = AclPlugRuntimeException.class)
    public void rangeNetaddressStrategyExceptionScopeTest() {
        rangeNetaddressStrategyExceptionTest("127.0.0.-1-200");
    }

    @Test(expected = AclPlugRuntimeException.class)
    public void rangeNetaddressStrategyExceptionScopeTwoTest() {
        rangeNetaddressStrategyExceptionTest("127.0.0.0-256");
    }

    private void rangeNetaddressStrategyExceptionTest(String netaddress) {
        PlainAccessResource plainAccessResource = new PlainAccessResource();
        plainAccessResource.setRemoteAddr(netaddress);
        netaddressStrategyFactory.getNetaddressStrategy(plainAccessResource);
    }

}
