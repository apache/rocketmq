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
package org.apache.rocketmq.tools.command.cluster;

import org.junit.Assert;
import org.junit.Test;

public class ClusterHealthRequestTest {
    @Test
    public void testDefaultsDescribeAllClusters() {
        ClusterHealthRequest request = new ClusterHealthRequest();

        request.validate();

        Assert.assertEquals("all-clusters", request.describeTarget());
        Assert.assertEquals(ClusterHealthRequest.DEFAULT_TIMEOUT_MILLIS, request.getTimeoutMillis());
        Assert.assertEquals(ClusterHealthRequest.DEFAULT_PARALLELISM, request.getParallelism());
        Assert.assertFalse(request.isDirectBrokerCheck());
    }

    @Test
    public void testBrokerAddressIsNormalized() {
        ClusterHealthRequest request = new ClusterHealthRequest();
        request.setBrokerAddr(" 127.0.0.1:10911 ");

        request.validate();

        Assert.assertTrue(request.isDirectBrokerCheck());
        Assert.assertEquals("127.0.0.1:10911", request.getBrokerAddr());
        Assert.assertEquals("broker:127.0.0.1:10911", request.describeTarget());
    }

    @Test
    public void testClusterNameIsNormalized() {
        ClusterHealthRequest request = new ClusterHealthRequest();
        request.setClusterName(" production ");

        request.validate();

        Assert.assertEquals("production", request.getClusterName());
        Assert.assertEquals("cluster:production", request.describeTarget());
    }

    @Test
    public void testNameServerOnlyTarget() {
        ClusterHealthRequest request = new ClusterHealthRequest();
        request.setNamesrvOnly(true);

        request.validate();

        Assert.assertEquals("nameserver", request.describeTarget());
    }

    @Test
    public void testRejectNonPositiveTimeout() {
        ClusterHealthRequest request = new ClusterHealthRequest();
        request.setTimeoutMillis(0);

        assertInvalid(request, "timeoutMillis");
    }

    @Test
    public void testRejectNonPositiveParallelism() {
        ClusterHealthRequest request = new ClusterHealthRequest();
        request.setParallelism(0);

        assertInvalid(request, "parallelism");
    }

    @Test
    public void testRejectExcessiveParallelism() {
        ClusterHealthRequest request = new ClusterHealthRequest();
        request.setParallelism(ClusterHealthRequest.MAX_PARALLELISM + 1);

        assertInvalid(request, "parallelism");
    }

    @Test
    public void testRejectBrokerAndClusterCombination() {
        ClusterHealthRequest request = new ClusterHealthRequest();
        request.setBrokerAddr("127.0.0.1:10911");
        request.setClusterName("production");

        assertInvalid(request, "cannot be used together");
    }

    @Test
    public void testRejectBrokerAndNameServerOnlyCombination() {
        ClusterHealthRequest request = new ClusterHealthRequest();
        request.setBrokerAddr("127.0.0.1:10911");
        request.setNamesrvOnly(true);

        assertInvalid(request, "cannot be used together");
    }

    @Test
    public void testRejectMastersForNameServerOnlyCheck() {
        ClusterHealthRequest request = new ClusterHealthRequest();
        request.setNamesrvOnly(true);
        request.setMastersOnly(true);

        assertInvalid(request, "mastersOnly");
    }

    @Test
    public void testRejectActiveRequirementForNameServerOnlyCheck() {
        ClusterHealthRequest request = new ClusterHealthRequest();
        request.setNamesrvOnly(true);
        request.setRequireActive(true);

        assertInvalid(request, "requireActive");
    }

    private static void assertInvalid(ClusterHealthRequest request, String messagePart) {
        try {
            request.validate();
            Assert.fail("Expected request validation to fail");
        } catch (IllegalArgumentException e) {
            Assert.assertTrue(e.getMessage(), e.getMessage().contains(messagePart));
        }
    }
}
