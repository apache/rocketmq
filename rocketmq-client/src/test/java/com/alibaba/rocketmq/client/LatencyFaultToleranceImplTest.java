/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.rocketmq.client;

import com.alibaba.rocketmq.client.latency.LatencyFaultToleranceImpl;
import org.junit.Assert;
import org.junit.Test;

public class LatencyFaultToleranceImplTest {
    @Test
    public void testNormal() throws InterruptedException {
        final LatencyFaultToleranceImpl fault = new LatencyFaultToleranceImpl();

        fault.updateFaultItem("broker1", 3, 0);

        fault.updateFaultItem("broker2", 5, 0);

        fault.updateFaultItem("broker3", 9, 0);

        fault.updateFaultItem("broker4", 400, 0);

        fault.updateFaultItem("broker5", 3000, 3000);

        fault.updateFaultItem("broker6", 3001, 3000);

        Assert.assertTrue(fault.isAvailable("broker1"));
        Assert.assertTrue(fault.isAvailable("broker2"));
        Assert.assertTrue(fault.isAvailable("broker3"));
        Assert.assertTrue(fault.isAvailable("broker4"));
        Assert.assertFalse(fault.isAvailable("broker5"));
        Thread.sleep(3000);
        Assert.assertTrue(fault.isAvailable("broker5"));
        Assert.assertTrue(fault.isAvailable("broker6"));

        System.out.println(fault);
    }

    @Test
    public void testFatal() throws InterruptedException {
        final LatencyFaultToleranceImpl fault = new LatencyFaultToleranceImpl();

        fault.updateFaultItem("broker1", 4000, 500);
        fault.updateFaultItem("broker2", 3000, 1000);
        fault.updateFaultItem("broker3", 200, 2000);
        fault.updateFaultItem("broker4", 200, 3000);
        fault.updateFaultItem("broker5", 50, 4000);
        fault.updateFaultItem("broker6", 50, 4000);

        Assert.assertFalse(fault.isAvailable("broker1"));
        Assert.assertFalse(fault.isAvailable("broker2"));
        Assert.assertFalse(fault.isAvailable("broker3"));
        Assert.assertFalse(fault.isAvailable("broker4"));
        Assert.assertFalse(fault.isAvailable("broker5"));

        for (int i = 0; i < 10; i++) {
            final String s = fault.pickOneAtLeast();
            System.out.println(s);
        }

        Thread.sleep(2000);

        for (int i = 0; i < 10; i++) {
            final String s = fault.pickOneAtLeast();
            System.out.println(s);
        }

        System.out.println(fault);
    }

    @Test
    public void testTwoBroker() throws InterruptedException {
        final LatencyFaultToleranceImpl fault = new LatencyFaultToleranceImpl();

        fault.updateFaultItem("broker1", 4000, 500);
        fault.updateFaultItem("broker2", 3000, 3000);


        Assert.assertFalse(fault.isAvailable("broker1"));
        Assert.assertFalse(fault.isAvailable("broker2"));


        for (int i = 0; i < 30; i++) {
            final String s = fault.pickOneAtLeast();
            System.out.println(s);
            Thread.sleep(300);
        }


        System.out.println(fault);
    }

    @Test
    public void testOneBroker() throws InterruptedException {
        final LatencyFaultToleranceImpl fault = new LatencyFaultToleranceImpl();

        fault.updateFaultItem("broker1", 4000, 500);



        Assert.assertFalse(fault.isAvailable("broker1"));
        Assert.assertTrue(fault.isAvailable("broker2"));


        for (int i = 0; i < 30; i++) {
            final String s = fault.pickOneAtLeast();
            System.out.println(s);
            Thread.sleep(300);
        }


        System.out.println(fault);
    }
}
