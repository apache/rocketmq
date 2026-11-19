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

import java.util.Arrays;
import java.util.Collections;
import org.junit.Assert;
import org.junit.Test;

public class ClusterHealthReportTest {
    @Test
    public void testCompleteHealthyReport() {
        ClusterHealthReport report = baseReport();
        report.setBrokers(Collections.singletonList(healthy("cluster-a", "broker-a", 0, "a:10911")));

        report.complete();

        Assert.assertTrue(report.isHealthy());
        Assert.assertEquals(1, report.getTotalBrokers());
        Assert.assertEquals(1, report.getHealthyBrokers());
        Assert.assertEquals(0, report.getUnhealthyBrokers());
    }

    @Test
    public void testBrokerFailureMakesReportUnhealthy() {
        ClusterHealthReport report = baseReport();
        BrokerTarget target = new BrokerTarget("cluster-a", "broker-b", 1, "b:10911");
        report.setBrokers(Arrays.asList(
            healthy("cluster-a", "broker-a", 0, "a:10911"),
            BrokerHealthResult.unhealthy(target, 12, "connection refused")));

        report.complete();

        Assert.assertFalse(report.isHealthy());
        Assert.assertEquals(2, report.getTotalBrokers());
        Assert.assertEquals(1, report.getHealthyBrokers());
        Assert.assertEquals(1, report.getUnhealthyBrokers());
    }

    @Test
    public void testNameServerFailureMakesReportUnhealthy() {
        ClusterHealthReport report = baseReport();
        report.setNameServerStatus(ClusterHealthReport.NameServerStatus.UNHEALTHY);
        report.setBrokers(Collections.emptyList());

        report.complete();

        Assert.assertFalse(report.isHealthy());
    }

    @Test
    public void testSkippedNameServerAllowsDirectBrokerSuccess() {
        ClusterHealthReport report = baseReport();
        report.setNameServerStatus(ClusterHealthReport.NameServerStatus.SKIPPED);
        report.setBrokers(Collections.singletonList(healthy("direct", "direct", -1, "a:10911")));

        report.complete();

        Assert.assertTrue(report.isHealthy());
    }

    @Test
    public void testExplicitNoBrokerFailureIsPreserved() {
        ClusterHealthReport report = baseReport();
        report.setBrokers(Collections.emptyList());
        report.markNoBrokers("No brokers matched cluster:missing");

        report.complete();

        Assert.assertFalse(report.isHealthy());
        Assert.assertTrue(report.getNameServerDetail().contains("No brokers matched"));
    }

    @Test
    public void testCompleteSortsBrokerRows() {
        ClusterHealthReport report = baseReport();
        report.setBrokers(Arrays.asList(
            healthy("cluster-b", "broker-a", 0, "c:10911"),
            healthy("cluster-a", "broker-b", 1, "b:10911"),
            healthy("cluster-a", "broker-a", 0, "a:10911")));

        report.complete();

        Assert.assertEquals("a:10911", report.getBrokers().get(0).getBrokerAddr());
        Assert.assertEquals("b:10911", report.getBrokers().get(1).getBrokerAddr());
        Assert.assertEquals("c:10911", report.getBrokers().get(2).getBrokerAddr());
    }

    @Test
    public void testTextContainsSummaryAndSanitizesDetail() {
        ClusterHealthReport report = baseReport();
        BrokerTarget target = new BrokerTarget("cluster-a", "broker-a", 0, "a:10911");
        report.setBrokers(Collections.singletonList(
            BrokerHealthResult.unhealthy(target, 9, "first line\nsecond line")));
        report.setDurationMillis(17);
        report.complete();

        String text = report.toText();

        Assert.assertTrue(text.contains("STATUS       UNHEALTHY"));
        Assert.assertTrue(text.contains("0 healthy, 1 unhealthy, 1 total, 17 ms"));
        Assert.assertTrue(text.contains("first line second line"));
        Assert.assertFalse(text.contains("first line\nsecond line"));
    }

    @Test
    public void testJsonContainsMachineReadableFields() {
        ClusterHealthReport report = baseReport();
        report.setBrokers(Collections.singletonList(healthy("cluster-a", "broker-a", 0, "a:10911")));
        report.complete();

        String json = report.toJson();

        Assert.assertTrue(json.contains("\"status\":\"HEALTHY\""));
        Assert.assertTrue(json.contains("\"nameServerStatus\":\"HEALTHY\""));
        Assert.assertTrue(json.contains("\"brokerAddr\":\"a:10911\""));
        Assert.assertTrue(json.contains("\"healthyBrokers\":1"));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testBrokerViewCannotBeMutated() {
        ClusterHealthReport report = baseReport();
        report.getBrokers().add(healthy("cluster-a", "broker-a", 0, "a:10911"));
    }

    private static ClusterHealthReport baseReport() {
        ClusterHealthReport report = new ClusterHealthReport();
        report.setTimestamp(1);
        report.setTarget("all-clusters");
        report.setNameServerStatus(ClusterHealthReport.NameServerStatus.HEALTHY);
        report.setNameServerDetail("metadata available");
        return report;
    }

    private static BrokerHealthResult healthy(String cluster, String broker, long brokerId, String address) {
        BrokerTarget target = new BrokerTarget(cluster, broker, brokerId, address);
        return BrokerHealthResult.healthy(target, 3, "V5_5_0", Boolean.TRUE);
    }
}
