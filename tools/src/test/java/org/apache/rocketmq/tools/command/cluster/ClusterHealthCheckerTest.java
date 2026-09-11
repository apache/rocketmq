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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.rocketmq.remoting.protocol.body.ClusterInfo;
import org.apache.rocketmq.remoting.protocol.body.KVTable;
import org.apache.rocketmq.remoting.protocol.route.BrokerData;
import org.junit.Assert;
import org.junit.Test;

public class ClusterHealthCheckerTest {
    @Test
    public void testDirectBrokerRpcSuccessIsHealthy() {
        FakeAdminAccess admin = new FakeAdminAccess();
        admin.response("127.0.0.1:10911", runtime("V5_5_0", "false"));
        ClusterHealthRequest request = directRequest("127.0.0.1:10911");

        ClusterHealthReport report = new ClusterHealthChecker(admin).check(request);

        Assert.assertTrue(report.isHealthy());
        Assert.assertEquals(ClusterHealthReport.NameServerStatus.SKIPPED, report.getNameServerStatus());
        Assert.assertEquals(1, report.getHealthyBrokers());
        Assert.assertEquals(Boolean.FALSE, report.getBrokers().get(0).getBrokerActive());
        Assert.assertEquals("V5_5_0", report.getBrokers().get(0).getBrokerVersion());
        Assert.assertEquals(0, admin.getDiscoveryCalls());
    }

    @Test
    public void testDirectBrokerFailureIsUnhealthy() {
        FakeAdminAccess admin = new FakeAdminAccess();
        admin.failure("127.0.0.1:10911", new IllegalStateException("connection refused"));

        ClusterHealthReport report = new ClusterHealthChecker(admin)
            .check(directRequest("127.0.0.1:10911"));

        Assert.assertFalse(report.isHealthy());
        Assert.assertEquals(1, report.getUnhealthyBrokers());
        Assert.assertTrue(report.getBrokers().get(0).getDetail().contains("connection refused"));
    }

    @Test
    public void testNullBrokerResponseIsUnhealthy() {
        FakeAdminAccess admin = new FakeAdminAccess();
        admin.response("127.0.0.1:10911", null);

        ClusterHealthReport report = new ClusterHealthChecker(admin)
            .check(directRequest("127.0.0.1:10911"));

        Assert.assertFalse(report.isHealthy());
        Assert.assertTrue(report.getBrokers().get(0).getDetail().contains("empty runtime response"));
    }

    @Test
    public void testMissingBrokerRuntimeTableIsUnhealthy() {
        FakeAdminAccess admin = new FakeAdminAccess();
        admin.response("127.0.0.1:10911", new KVTable());

        ClusterHealthReport report = new ClusterHealthChecker(admin)
            .check(directRequest("127.0.0.1:10911"));

        Assert.assertFalse(report.isHealthy());
        Assert.assertTrue(report.getBrokers().get(0).getDetail().contains("empty runtime response"));
    }

    @Test
    public void testRequireActiveRejectsFalseFlag() {
        FakeAdminAccess admin = new FakeAdminAccess();
        admin.response("127.0.0.1:10911", runtime("V5_5_0", "false"));
        ClusterHealthRequest request = directRequest("127.0.0.1:10911");
        request.setRequireActive(true);

        ClusterHealthReport report = new ClusterHealthChecker(admin).check(request);

        Assert.assertFalse(report.isHealthy());
        Assert.assertEquals("brokerActive is false", report.getBrokers().get(0).getDetail());
        Assert.assertEquals("V5_5_0", report.getBrokers().get(0).getBrokerVersion());
    }

    @Test
    public void testRequireActiveRejectsMissingFlag() {
        FakeAdminAccess admin = new FakeAdminAccess();
        admin.response("127.0.0.1:10911", runtime("V5_5_0", null));
        ClusterHealthRequest request = directRequest("127.0.0.1:10911");
        request.setRequireActive(true);

        ClusterHealthReport report = new ClusterHealthChecker(admin).check(request);

        Assert.assertFalse(report.isHealthy());
        Assert.assertTrue(report.getBrokers().get(0).getDetail().contains("missing"));
    }

    @Test
    public void testRequireActiveAcceptsTrueFlagIgnoringCase() {
        FakeAdminAccess admin = new FakeAdminAccess();
        admin.response("127.0.0.1:10911", runtime("V5_5_0", "TRUE"));
        ClusterHealthRequest request = directRequest("127.0.0.1:10911");
        request.setRequireActive(true);

        ClusterHealthReport report = new ClusterHealthChecker(admin).check(request);

        Assert.assertTrue(report.isHealthy());
        Assert.assertEquals(Boolean.TRUE, report.getBrokers().get(0).getBrokerActive());
    }

    @Test
    public void testNameServerOnlyAllowsEmptyCluster() {
        FakeAdminAccess admin = new FakeAdminAccess();
        admin.setClusterInfo(emptyClusterInfo());
        ClusterHealthRequest request = new ClusterHealthRequest();
        request.setNamesrvOnly(true);

        ClusterHealthReport report = new ClusterHealthChecker(admin).check(request);

        Assert.assertTrue(report.isHealthy());
        Assert.assertEquals(ClusterHealthReport.NameServerStatus.HEALTHY, report.getNameServerStatus());
        Assert.assertEquals(0, report.getTotalBrokers());
        Assert.assertEquals(1, admin.getDiscoveryCalls());
    }

    @Test
    public void testNameServerExceptionIsUnhealthy() {
        FakeAdminAccess admin = new FakeAdminAccess();
        admin.setDiscoveryFailure(new IllegalStateException("nameserver unavailable"));

        ClusterHealthReport report = new ClusterHealthChecker(admin).check(new ClusterHealthRequest());

        Assert.assertFalse(report.isHealthy());
        Assert.assertEquals(ClusterHealthReport.NameServerStatus.UNHEALTHY, report.getNameServerStatus());
        Assert.assertTrue(report.getNameServerDetail().contains("nameserver unavailable"));
        Assert.assertEquals(0, admin.getProbeCalls());
    }

    @Test
    public void testNullNameServerResponseIsUnhealthy() {
        FakeAdminAccess admin = new FakeAdminAccess();
        admin.setClusterInfo(null);

        ClusterHealthReport report = new ClusterHealthChecker(admin).check(new ClusterHealthRequest());

        Assert.assertFalse(report.isHealthy());
        Assert.assertTrue(report.getNameServerDetail().contains("empty cluster response"));
    }

    @Test
    public void testAllClusterBrokersAreProbed() {
        FakeAdminAccess admin = twoClusterAdmin();

        ClusterHealthReport report = new ClusterHealthChecker(admin).check(new ClusterHealthRequest());

        Assert.assertTrue(report.isHealthy());
        Assert.assertEquals(4, report.getTotalBrokers());
        Assert.assertEquals(4, admin.getProbeCalls());
        Assert.assertEquals("a-master:10911", report.getBrokers().get(0).getBrokerAddr());
    }

    @Test
    public void testClusterFilterOnlyProbesMatchingCluster() {
        FakeAdminAccess admin = twoClusterAdmin();
        ClusterHealthRequest request = new ClusterHealthRequest();
        request.setClusterName("cluster-b");

        ClusterHealthReport report = new ClusterHealthChecker(admin).check(request);

        Assert.assertTrue(report.isHealthy());
        Assert.assertEquals(1, report.getTotalBrokers());
        Assert.assertEquals("cluster-b", report.getBrokers().get(0).getClusterName());
        Assert.assertEquals("b-master:10911", report.getBrokers().get(0).getBrokerAddr());
    }

    @Test
    public void testMissingClusterIsUnhealthy() {
        FakeAdminAccess admin = twoClusterAdmin();
        ClusterHealthRequest request = new ClusterHealthRequest();
        request.setClusterName("missing");

        ClusterHealthReport report = new ClusterHealthChecker(admin).check(request);

        Assert.assertFalse(report.isHealthy());
        Assert.assertEquals(0, report.getTotalBrokers());
        Assert.assertTrue(report.getNameServerDetail().contains("No brokers matched"));
    }

    @Test
    public void testMastersOnlySkipsSlaveAddresses() {
        FakeAdminAccess admin = twoClusterAdmin();
        ClusterHealthRequest request = new ClusterHealthRequest();
        request.setMastersOnly(true);

        ClusterHealthReport report = new ClusterHealthChecker(admin).check(request);

        Assert.assertTrue(report.isHealthy());
        Assert.assertEquals(3, report.getTotalBrokers());
        for (BrokerHealthResult broker : report.getBrokers()) {
            Assert.assertEquals(0L, broker.getBrokerId());
        }
        Assert.assertFalse(admin.getProbedAddresses().contains("a-slave:10911"));
    }

    @Test
    public void testOneBrokerFailureFailsWholeCluster() {
        FakeAdminAccess admin = twoClusterAdmin();
        admin.failure("a-slave:10911", new IllegalStateException("not reachable"));

        ClusterHealthReport report = new ClusterHealthChecker(admin).check(new ClusterHealthRequest());

        Assert.assertFalse(report.isHealthy());
        Assert.assertEquals(3, report.getHealthyBrokers());
        Assert.assertEquals(1, report.getUnhealthyBrokers());
    }

    @Test(timeout = 3000)
    public void testOverallDeadlineMarksSlowProbeAsTimedOut() {
        FakeAdminAccess admin = new FakeAdminAccess();
        admin.setClusterInfo(clusterInfo("cluster-a", broker("cluster-a", "broker-a",
            address(0, "fast:10911", 1, "slow:10911"))));
        admin.response("fast:10911", runtime("V5_5_0", "true"));
        admin.delay("slow:10911", 500);
        admin.response("slow:10911", runtime("V5_5_0", "true"));
        ClusterHealthRequest request = new ClusterHealthRequest();
        request.setTimeoutMillis(50);
        request.setParallelism(2);

        ClusterHealthReport report = new ClusterHealthChecker(admin).check(request);

        Assert.assertFalse(report.isHealthy());
        Assert.assertEquals(1, report.getHealthyBrokers());
        Assert.assertEquals(1, report.getUnhealthyBrokers());
        Assert.assertTrue(find(report, "slow:10911").getDetail().contains("timed out"));
    }

    @Test
    public void testMalformedClusterMetadataIsHandled() {
        FakeAdminAccess admin = new FakeAdminAccess();
        ClusterInfo clusterInfo = new ClusterInfo();
        clusterInfo.setClusterAddrTable(null);
        clusterInfo.setBrokerAddrTable(null);
        admin.setClusterInfo(clusterInfo);

        ClusterHealthReport report = new ClusterHealthChecker(admin).check(new ClusterHealthRequest());

        Assert.assertFalse(report.isHealthy());
        Assert.assertEquals(0, report.getTotalBrokers());
    }

    @Test
    public void testSelectTargetsSkipsMissingBrokerDataAndBlankAddress() {
        FakeAdminAccess admin = new FakeAdminAccess();
        ClusterInfo clusterInfo = new ClusterInfo();
        Map<String, Set<String>> clusters = new HashMap<>();
        clusters.put("cluster-a", new HashSet<>(Arrays.asList("missing", "blank", "valid")));
        clusterInfo.setClusterAddrTable(clusters);
        Map<String, BrokerData> brokers = new HashMap<>();
        brokers.put("blank", broker("cluster-a", "blank", address(0, " ")));
        brokers.put("valid", broker("cluster-a", "valid", address(0, "valid:10911")));
        clusterInfo.setBrokerAddrTable(brokers);
        admin.setClusterInfo(clusterInfo);
        admin.response("valid:10911", runtime("V5_5_0", "true"));

        ClusterHealthReport report = new ClusterHealthChecker(admin).check(new ClusterHealthRequest());

        Assert.assertTrue(report.isHealthy());
        Assert.assertEquals(1, report.getTotalBrokers());
        Assert.assertEquals("valid:10911", report.getBrokers().get(0).getBrokerAddr());
    }

    private static BrokerHealthResult find(ClusterHealthReport report, String address) {
        for (BrokerHealthResult result : report.getBrokers()) {
            if (address.equals(result.getBrokerAddr())) {
                return result;
            }
        }
        throw new AssertionError("No broker result for " + address);
    }

    private static ClusterHealthRequest directRequest(String address) {
        ClusterHealthRequest request = new ClusterHealthRequest();
        request.setBrokerAddr(address);
        return request;
    }

    private static FakeAdminAccess twoClusterAdmin() {
        BrokerData brokerA = broker("cluster-a", "broker-a",
            address(0, "a-master:10911", 1, "a-slave:10911"));
        BrokerData brokerA2 = broker("cluster-a", "broker-a2", address(0, "a2-master:10911"));
        BrokerData brokerB = broker("cluster-b", "broker-b", address(0, "b-master:10911"));
        ClusterInfo clusterInfo = clusterInfo(
            new String[] {"cluster-a", "cluster-a", "cluster-b"}, brokerA, brokerA2, brokerB);
        FakeAdminAccess admin = new FakeAdminAccess();
        admin.setClusterInfo(clusterInfo);
        for (String address : Arrays.asList(
            "a-master:10911", "a-slave:10911", "a2-master:10911", "b-master:10911")) {
            admin.response(address, runtime("V5_5_0", "true"));
        }
        return admin;
    }

    private static ClusterInfo emptyClusterInfo() {
        ClusterInfo clusterInfo = new ClusterInfo();
        clusterInfo.setClusterAddrTable(new HashMap<>());
        clusterInfo.setBrokerAddrTable(new HashMap<>());
        return clusterInfo;
    }

    private static ClusterInfo clusterInfo(String cluster, BrokerData... brokers) {
        String[] clusters = new String[brokers.length];
        Arrays.fill(clusters, cluster);
        return clusterInfo(clusters, brokers);
    }

    private static ClusterInfo clusterInfo(String[] clusterNames, BrokerData... brokers) {
        ClusterInfo clusterInfo = new ClusterInfo();
        Map<String, Set<String>> clusters = new HashMap<>();
        Map<String, BrokerData> brokerTable = new HashMap<>();
        for (int i = 0; i < brokers.length; i++) {
            BrokerData broker = brokers[i];
            clusters.computeIfAbsent(clusterNames[i], ignored -> new HashSet<>())
                .add(broker.getBrokerName());
            brokerTable.put(broker.getBrokerName(), broker);
        }
        clusterInfo.setClusterAddrTable(clusters);
        clusterInfo.setBrokerAddrTable(brokerTable);
        return clusterInfo;
    }

    private static BrokerData broker(String cluster, String brokerName, HashMap<Long, String> addresses) {
        return new BrokerData(cluster, brokerName, addresses);
    }

    private static HashMap<Long, String> address(Object... values) {
        HashMap<Long, String> addresses = new HashMap<>();
        for (int i = 0; i < values.length; i += 2) {
            addresses.put(((Number) values[i]).longValue(), (String) values[i + 1]);
        }
        return addresses;
    }

    private static KVTable runtime(String version, String active) {
        KVTable table = new KVTable();
        HashMap<String, String> values = new HashMap<>();
        if (version != null) {
            values.put("brokerVersionDesc", version);
        }
        if (active != null) {
            values.put("brokerActive", active);
        }
        table.setTable(values);
        return table;
    }

    private static class FakeAdminAccess implements ClusterHealthChecker.AdminAccess {
        private ClusterInfo clusterInfo = emptyClusterInfo();
        private Exception discoveryFailure;
        private final Map<String, KVTable> responses = new ConcurrentHashMap<>();
        private final Map<String, Exception> failures = new ConcurrentHashMap<>();
        private final Map<String, Long> delays = new ConcurrentHashMap<>();
        private final Set<String> probedAddresses = ConcurrentHashMap.newKeySet();
        private int discoveryCalls;

        @Override
        public ClusterInfo examineBrokerClusterInfo() throws Exception {
            discoveryCalls++;
            if (discoveryFailure != null) {
                throw discoveryFailure;
            }
            return clusterInfo;
        }

        @Override
        public KVTable fetchBrokerRuntimeStats(String brokerAddr) throws Exception {
            probedAddresses.add(brokerAddr);
            Long delay = delays.get(brokerAddr);
            if (delay != null) {
                Thread.sleep(delay);
            }
            Exception failure = failures.get(brokerAddr);
            if (failure != null) {
                throw failure;
            }
            return responses.get(brokerAddr);
        }

        void setClusterInfo(ClusterInfo clusterInfo) {
            this.clusterInfo = clusterInfo;
        }

        void setDiscoveryFailure(Exception discoveryFailure) {
            this.discoveryFailure = discoveryFailure;
        }

        void response(String address, KVTable response) {
            if (response == null) {
                responses.remove(address);
            } else {
                responses.put(address, response);
            }
        }

        void failure(String address, Exception failure) {
            failures.put(address, failure);
        }

        void delay(String address, long delayMillis) {
            delays.put(address, delayMillis);
        }

        int getDiscoveryCalls() {
            return discoveryCalls;
        }

        int getProbeCalls() {
            return probedAddresses.size();
        }

        Set<String> getProbedAddresses() {
            return Collections.unmodifiableSet(probedAddresses);
        }
    }
}
