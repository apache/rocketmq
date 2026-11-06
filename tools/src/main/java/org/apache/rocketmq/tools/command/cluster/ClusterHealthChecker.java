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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.remoting.protocol.body.ClusterInfo;
import org.apache.rocketmq.remoting.protocol.body.KVTable;
import org.apache.rocketmq.remoting.protocol.route.BrokerData;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;

public class ClusterHealthChecker {
    public interface AdminAccess {
        ClusterInfo examineBrokerClusterInfo() throws Exception;

        KVTable fetchBrokerRuntimeStats(String brokerAddr) throws Exception;
    }

    public static class DefaultAdminAccess implements AdminAccess {
        private final DefaultMQAdminExt adminExt;

        public DefaultAdminAccess(DefaultMQAdminExt adminExt) {
            this.adminExt = adminExt;
        }

        @Override
        public ClusterInfo examineBrokerClusterInfo() throws Exception {
            return adminExt.examineBrokerClusterInfo();
        }

        @Override
        public KVTable fetchBrokerRuntimeStats(String brokerAddr) throws Exception {
            return adminExt.fetchBrokerRuntimeStats(brokerAddr);
        }
    }

    private final AdminAccess adminAccess;

    public ClusterHealthChecker(AdminAccess adminAccess) {
        this.adminAccess = adminAccess;
    }

    public ClusterHealthReport check(ClusterHealthRequest request) {
        request.validate();
        long begin = System.currentTimeMillis();
        ClusterHealthReport report = new ClusterHealthReport();
        report.setTimestamp(begin);
        report.setTarget(request.describeTarget());

        if (request.isDirectBrokerCheck()) {
            checkDirectBroker(request, report);
        } else {
            checkThroughNameServer(request, report);
        }

        report.setDurationMillis(Math.max(0, System.currentTimeMillis() - begin));
        report.complete();
        return report;
    }

    private void checkDirectBroker(ClusterHealthRequest request, ClusterHealthReport report) {
        report.setNameServerStatus(ClusterHealthReport.NameServerStatus.SKIPPED);
        report.setNameServerDetail("Direct broker check");
        BrokerTarget target = BrokerTarget.direct(request.getBrokerAddr());
        report.setBrokers(Collections.singletonList(probe(target, request.isRequireActive())));
    }

    private void checkThroughNameServer(ClusterHealthRequest request, ClusterHealthReport report) {
        ClusterInfo clusterInfo;
        long begin = System.currentTimeMillis();
        try {
            clusterInfo = adminAccess.examineBrokerClusterInfo();
            if (clusterInfo == null) {
                throw new IllegalStateException("NameServer returned an empty cluster response");
            }
            report.setNameServerStatus(ClusterHealthReport.NameServerStatus.HEALTHY);
            report.setNameServerDetail("Cluster metadata RPC succeeded in "
                + Math.max(0, System.currentTimeMillis() - begin) + " ms");
        } catch (Exception e) {
            report.setNameServerStatus(ClusterHealthReport.NameServerStatus.UNHEALTHY);
            report.setNameServerDetail(describeFailure(e));
            report.setBrokers(Collections.emptyList());
            return;
        }

        if (request.isNamesrvOnly()) {
            report.setBrokers(Collections.emptyList());
            return;
        }

        List<BrokerTarget> targets = selectTargets(clusterInfo, request);
        if (targets.isEmpty()) {
            report.setBrokers(Collections.emptyList());
            report.markNoBrokers("No brokers matched " + request.describeTarget());
            return;
        }
        report.setBrokers(probeAll(targets, request));
    }

    List<BrokerTarget> selectTargets(ClusterInfo clusterInfo, ClusterHealthRequest request) {
        Map<String, Set<String>> clusterTable = clusterInfo.getClusterAddrTable();
        Map<String, BrokerData> brokerTable = clusterInfo.getBrokerAddrTable();
        if (clusterTable == null || brokerTable == null) {
            return Collections.emptyList();
        }

        Set<String> requestedClusters = new HashSet<>();
        if (hasText(request.getClusterName())) {
            requestedClusters.add(request.getClusterName());
        } else {
            requestedClusters.addAll(clusterTable.keySet());
        }

        List<BrokerTarget> targets = new ArrayList<>();
        Set<String> identities = new HashSet<>();
        for (String clusterName : requestedClusters) {
            Set<String> brokerNames = clusterTable.get(clusterName);
            if (brokerNames == null) {
                continue;
            }
            for (String brokerName : brokerNames) {
                BrokerData brokerData = brokerTable.get(brokerName);
                if (brokerData == null || brokerData.getBrokerAddrs() == null) {
                    continue;
                }
                for (Map.Entry<Long, String> entry : brokerData.getBrokerAddrs().entrySet()) {
                    if (request.isMastersOnly() && entry.getKey() != MixAll.MASTER_ID) {
                        continue;
                    }
                    if (!hasText(entry.getValue())) {
                        continue;
                    }
                    String identity = clusterName + '\u0000' + brokerName + '\u0000' + entry.getKey()
                        + '\u0000' + entry.getValue();
                    if (identities.add(identity)) {
                        targets.add(new BrokerTarget(clusterName, brokerName, entry.getKey(), entry.getValue()));
                    }
                }
            }
        }
        targets.sort(BrokerTarget.COMPARATOR);
        return targets;
    }

    private List<BrokerHealthResult> probeAll(List<BrokerTarget> targets, ClusterHealthRequest request) {
        int threadCount = Math.min(request.getParallelism(), targets.size());
        ExecutorService executor = Executors.newFixedThreadPool(threadCount, new HealthCheckThreadFactory());
        CompletionService<BrokerHealthResult> completionService = new ExecutorCompletionService<>(executor);
        List<Future<BrokerHealthResult>> futures = new ArrayList<>();
        for (BrokerTarget target : targets) {
            futures.add(completionService.submit(new ProbeTask(target, request.isRequireActive())));
        }

        List<BrokerHealthResult> results = new ArrayList<>();
        long deadlineNanos = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(request.getTimeoutMillis());
        try {
            while (results.size() < targets.size()) {
                long remainingNanos = deadlineNanos - System.nanoTime();
                if (remainingNanos <= 0) {
                    break;
                }
                Future<BrokerHealthResult> completed = completionService.poll(remainingNanos, TimeUnit.NANOSECONDS);
                if (completed == null) {
                    break;
                }
                try {
                    results.add(completed.get());
                } catch (Exception e) {
                    // ProbeTask converts broker failures into results. This branch only guards executor failures.
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            for (Future<BrokerHealthResult> future : futures) {
                future.cancel(true);
            }
            executor.shutdownNow();
        }

        Set<String> completedAddresses = new HashSet<>();
        for (BrokerHealthResult result : results) {
            completedAddresses.add(identity(result));
        }
        for (BrokerTarget target : targets) {
            if (!completedAddresses.contains(identity(target))) {
                results.add(BrokerHealthResult.unhealthy(target, request.getTimeoutMillis(),
                    "Health check timed out after " + request.getTimeoutMillis() + " ms"));
            }
        }
        return results;
    }

    private BrokerHealthResult probe(BrokerTarget target, boolean requireActive) {
        long begin = System.currentTimeMillis();
        try {
            KVTable runtime = adminAccess.fetchBrokerRuntimeStats(target.getBrokerAddr());
            long latency = Math.max(0, System.currentTimeMillis() - begin);
            if (runtime == null || runtime.getTable() == null || runtime.getTable().isEmpty()) {
                return BrokerHealthResult.unhealthy(target, latency, "Broker returned an empty runtime response");
            }

            String brokerVersion = runtime.getTable().get("brokerVersionDesc");
            Boolean brokerActive = parseBoolean(runtime.getTable().get("brokerActive"));
            if (requireActive && !Boolean.TRUE.equals(brokerActive)) {
                String detail = brokerActive == null
                    ? "brokerActive is missing from the runtime response"
                    : "brokerActive is false";
                BrokerHealthResult result = BrokerHealthResult.unhealthy(target, latency, detail);
                result.setBrokerVersion(brokerVersion);
                result.setBrokerActive(brokerActive);
                return result;
            }
            return BrokerHealthResult.healthy(target, latency, brokerVersion, brokerActive);
        } catch (Exception e) {
            return BrokerHealthResult.unhealthy(target,
                Math.max(0, System.currentTimeMillis() - begin), describeFailure(e));
        }
    }

    private static Boolean parseBoolean(String value) {
        if (value == null) {
            return null;
        }
        if ("true".equalsIgnoreCase(value)) {
            return Boolean.TRUE;
        }
        if ("false".equalsIgnoreCase(value)) {
            return Boolean.FALSE;
        }
        return null;
    }

    private static String describeFailure(Throwable throwable) {
        Throwable root = throwable;
        while (root.getCause() != null && root.getCause() != root) {
            root = root.getCause();
        }
        String message = root.getMessage();
        if (!hasText(message)) {
            return root.getClass().getSimpleName();
        }
        return root.getClass().getSimpleName() + ": " + message;
    }

    private static String identity(BrokerHealthResult result) {
        return result.getClusterName() + '\u0000' + result.getBrokerName() + '\u0000'
            + result.getBrokerId() + '\u0000' + result.getBrokerAddr();
    }

    private static String identity(BrokerTarget target) {
        return target.getClusterName() + '\u0000' + target.getBrokerName() + '\u0000'
            + target.getBrokerId() + '\u0000' + target.getBrokerAddr();
    }

    private static boolean hasText(String value) {
        return value != null && !value.trim().isEmpty();
    }

    private class ProbeTask implements Callable<BrokerHealthResult> {
        private final BrokerTarget target;
        private final boolean requireActive;

        private ProbeTask(BrokerTarget target, boolean requireActive) {
            this.target = target;
            this.requireActive = requireActive;
        }

        @Override
        public BrokerHealthResult call() {
            return probe(target, requireActive);
        }
    }

    private static class HealthCheckThreadFactory implements ThreadFactory {
        private final AtomicInteger counter = new AtomicInteger();

        @Override
        public Thread newThread(Runnable runnable) {
            Thread thread = new Thread(runnable, "ClusterHealthCheck_" + counter.incrementAndGet());
            thread.setDaemon(true);
            return thread;
        }
    }
}
