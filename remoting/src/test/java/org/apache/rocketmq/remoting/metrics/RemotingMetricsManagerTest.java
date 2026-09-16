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
package org.apache.rocketmq.remoting.metrics;

import io.opentelemetry.api.common.Attributes;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class RemotingMetricsManagerTest {

    private RemotingMetricsManager manager;

    @Before
    public void setUp() {
        manager = new RemotingMetricsManager();
    }

    @Test
    public void testCacheHitSameArgs() {
        Attributes a1 = manager.getOrBuildAttributes(10, 200, false, RemotingMetricsConstant.RESULT_SUCCESS);
        Attributes a2 = manager.getOrBuildAttributes(10, 200, false, RemotingMetricsConstant.RESULT_SUCCESS);
        Assert.assertSame("Same args should return cached instance", a1, a2);
    }

    @Test
    public void testDifferentRequestCode() {
        Attributes a = manager.getOrBuildAttributes(10, 200, false, RemotingMetricsConstant.RESULT_SUCCESS);
        Attributes b = manager.getOrBuildAttributes(20, 200, false, RemotingMetricsConstant.RESULT_SUCCESS);
        Assert.assertNotSame("Different requestCode should produce different Attributes", a, b);
    }

    @Test
    public void testDifferentResponseCode() {
        Attributes a = manager.getOrBuildAttributes(10, 200, false, RemotingMetricsConstant.RESULT_SUCCESS);
        Attributes b = manager.getOrBuildAttributes(10, 500, false, RemotingMetricsConstant.RESULT_SUCCESS);
        Assert.assertNotSame("Different responseCode should produce different Attributes", a, b);
    }

    @Test
    public void testDifferentIsLongPolling() {
        Attributes a = manager.getOrBuildAttributes(10, 200, false, RemotingMetricsConstant.RESULT_SUCCESS);
        Attributes b = manager.getOrBuildAttributes(10, 200, true, RemotingMetricsConstant.RESULT_SUCCESS);
        Assert.assertNotSame("Different isLongPolling should produce different Attributes", a, b);
    }

    @Test
    public void testDifferentResult() {
        Attributes success = manager.getOrBuildAttributes(10, 200, false, RemotingMetricsConstant.RESULT_SUCCESS);
        Attributes oneway = manager.getOrBuildAttributes(10, 200, false, RemotingMetricsConstant.RESULT_ONEWAY);
        Assert.assertNotSame("Different result should produce different Attributes", success, oneway);
    }

    @Test
    public void testUnknownResultNotCached() {
        Attributes a1 = manager.getOrBuildAttributes(10, 200, false, "UNKNOWN_RESULT");
        Attributes a2 = manager.getOrBuildAttributes(10, 200, false, "UNKNOWN_RESULT");
        Assert.assertNotSame("Unknown result should not be cached (returns new instance each time)", a1, a2);
    }

    @Test
    public void testKnownResultsCached() {
        String[] results = {
            RemotingMetricsConstant.RESULT_SUCCESS,
            RemotingMetricsConstant.RESULT_ONEWAY,
            RemotingMetricsConstant.RESULT_WRITE_CHANNEL_FAILED,
            RemotingMetricsConstant.RESULT_CANCELED
        };
        for (String result : results) {
            Attributes a1 = manager.getOrBuildAttributes(10, 200, false, result);
            Attributes a2 = manager.getOrBuildAttributes(10, 200, false, result);
            Assert.assertSame("Result " + result + " should be cached", a1, a2);
        }
    }
}
