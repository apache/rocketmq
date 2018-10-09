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
package org.apache.rocketmq.acl.plug.strategy;

import org.apache.rocketmq.acl.plug.entity.AccessControl;
import org.apache.rocketmq.acl.plug.exception.AclPlugAccountAnalysisException;
import org.junit.Assert;
import org.junit.Test;

public class NetaddressStrategyTest {

    NetaddressStrategyFactory netaddressStrategyFactory = new NetaddressStrategyFactory();

    @Test
    public void NetaddressStrategyFactoryTest() {
        AccessControl accessControl = new AccessControl();
        NetaddressStrategy netaddressStrategy = netaddressStrategyFactory.getNetaddressStrategy(accessControl);
        Assert.assertEquals(netaddressStrategy, NullNetaddressStrategy.NULL_NET_ADDRESS_STRATEGY);

        accessControl.setNetaddress("*");
        netaddressStrategy = netaddressStrategyFactory.getNetaddressStrategy(accessControl);
        Assert.assertEquals(netaddressStrategy, NullNetaddressStrategy.NULL_NET_ADDRESS_STRATEGY);

        accessControl.setNetaddress("127.0.0.1");
        netaddressStrategy = netaddressStrategyFactory.getNetaddressStrategy(accessControl);
        Assert.assertEquals(netaddressStrategy.getClass(), OneNetaddressStrategy.class);

        accessControl.setNetaddress("127.0.0.1,127.0.0.2,127.0.0.3");
        netaddressStrategy = netaddressStrategyFactory.getNetaddressStrategy(accessControl);
        Assert.assertEquals(netaddressStrategy.getClass(), MultipleNetaddressStrategy.class);

        accessControl.setNetaddress("127.0.0.{1,2,3}");
        netaddressStrategy = netaddressStrategyFactory.getNetaddressStrategy(accessControl);
        Assert.assertEquals(netaddressStrategy.getClass(), MultipleNetaddressStrategy.class);

        accessControl.setNetaddress("127.0.0.1-200");
        netaddressStrategy = netaddressStrategyFactory.getNetaddressStrategy(accessControl);
        Assert.assertEquals(netaddressStrategy.getClass(), RangeNetaddressStrategy.class);

        accessControl.setNetaddress("127.0.0.*");
        netaddressStrategy = netaddressStrategyFactory.getNetaddressStrategy(accessControl);
        Assert.assertEquals(netaddressStrategy.getClass(), RangeNetaddressStrategy.class);

        accessControl.setNetaddress("127.0.1-20.*");
        netaddressStrategy = netaddressStrategyFactory.getNetaddressStrategy(accessControl);
        Assert.assertEquals(netaddressStrategy.getClass(), RangeNetaddressStrategy.class);
    }

    @Test(expected = AclPlugAccountAnalysisException.class)
    public void verifyTest() {
        new OneNetaddressStrategy("127.0.0.1");

        new OneNetaddressStrategy("256.0.0.1");
    }

    @Test
    public void nullNetaddressStrategyTest() {
        boolean isMatch = NullNetaddressStrategy.NULL_NET_ADDRESS_STRATEGY.match(new AccessControl());
        Assert.assertTrue(isMatch);
    }

    public void oneNetaddressStrategyTest() {
        OneNetaddressStrategy netaddressStrategy = new OneNetaddressStrategy("127.0.0.1");
        AccessControl accessControl = new AccessControl();
        boolean match = netaddressStrategy.match(accessControl);
        Assert.assertFalse(match);

        accessControl.setNetaddress("127.0.0.2");
        match = netaddressStrategy.match(accessControl);
        Assert.assertFalse(match);

        accessControl.setNetaddress("127.0.0.1");
        match = netaddressStrategy.match(accessControl);
        Assert.assertTrue(match);
    }

    @Test
    public void multipleNetaddressStrategyTest() {
        AccessControl accessControl = new AccessControl();
        accessControl.setNetaddress("127.0.0.1,127.0.0.2,127.0.0.3");
        NetaddressStrategy netaddressStrategy = netaddressStrategyFactory.getNetaddressStrategy(accessControl);
        multipleNetaddressStrategyTest(netaddressStrategy);

        accessControl.setNetaddress("127.0.0.{1,2,3}");
        netaddressStrategy = netaddressStrategyFactory.getNetaddressStrategy(accessControl);
        multipleNetaddressStrategyTest(netaddressStrategy);

    }

    @Test(expected = AclPlugAccountAnalysisException.class)
    public void multipleNetaddressStrategyExceptionTest() {
        AccessControl accessControl = new AccessControl();
        accessControl.setNetaddress("127.0.0.1,2,3}");
        netaddressStrategyFactory.getNetaddressStrategy(accessControl);
    }

    private void multipleNetaddressStrategyTest(NetaddressStrategy netaddressStrategy) {
        AccessControl accessControl = new AccessControl();
        accessControl.setNetaddress("127.0.0.1");
        boolean match = netaddressStrategy.match(accessControl);
        Assert.assertTrue(match);

        accessControl.setNetaddress("127.0.0.2");
        match = netaddressStrategy.match(accessControl);
        Assert.assertTrue(match);

        accessControl.setNetaddress("127.0.0.3");
        match = netaddressStrategy.match(accessControl);
        Assert.assertTrue(match);

        accessControl.setNetaddress("127.0.0.4");
        match = netaddressStrategy.match(accessControl);
        Assert.assertFalse(match);

        accessControl.setNetaddress("127.0.0.0");
        match = netaddressStrategy.match(accessControl);
        Assert.assertFalse(match);

    }

    @Test
    public void rangeNetaddressStrategyTest() {
        String head = "127.0.0.";
        AccessControl accessControl = new AccessControl();
        accessControl.setNetaddress("127.0.0.1-200");
        NetaddressStrategy netaddressStrategy = netaddressStrategyFactory.getNetaddressStrategy(accessControl);
        rangeNetaddressStrategyTest(netaddressStrategy, head, 1, 200, true);
        accessControl.setNetaddress("127.0.0.*");
        netaddressStrategy = netaddressStrategyFactory.getNetaddressStrategy(accessControl);
        rangeNetaddressStrategyTest(netaddressStrategy, head, 0, 255, true);

        accessControl.setNetaddress("127.0.1-200.*");
        netaddressStrategy = netaddressStrategyFactory.getNetaddressStrategy(accessControl);
        rangeNetaddressStrategyThirdlyTest(netaddressStrategy, head, 1, 200);
    }

    private void rangeNetaddressStrategyTest(NetaddressStrategy netaddressStrategy, String head, int start, int end,
        boolean isFalse) {
        AccessControl accessControl = new AccessControl();
        for (int i = -10; i < 300; i++) {
            accessControl.setNetaddress(head + i);
            boolean match = netaddressStrategy.match(accessControl);
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

    @Test(expected = AclPlugAccountAnalysisException.class)
    public void rangeNetaddressStrategyExceptionStartGreaterEndTest() {
        rangeNetaddressStrategyExceptionTest("127.0.0.2-1");
    }

    @Test(expected = AclPlugAccountAnalysisException.class)
    public void rangeNetaddressStrategyExceptionScopeTest() {
        rangeNetaddressStrategyExceptionTest("127.0.0.-1-200");
    }

    @Test(expected = AclPlugAccountAnalysisException.class)
    public void rangeNetaddressStrategyExceptionScopeTwoTest() {
        rangeNetaddressStrategyExceptionTest("127.0.0.0-256");
    }

    private void rangeNetaddressStrategyExceptionTest(String netaddress) {
        AccessControl accessControl = new AccessControl();
        accessControl.setNetaddress(netaddress);
        netaddressStrategyFactory.getNetaddressStrategy(accessControl);
    }

}
