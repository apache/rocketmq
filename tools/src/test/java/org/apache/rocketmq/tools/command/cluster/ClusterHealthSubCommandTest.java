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

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.HashMap;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.rocketmq.remoting.protocol.body.ClusterInfo;
import org.apache.rocketmq.remoting.protocol.body.KVTable;
import org.apache.rocketmq.srvutil.ServerUtil;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.junit.Assert;
import org.junit.Test;

public class ClusterHealthSubCommandTest {
    @Test
    public void testCommandIdentity() {
        ClusterHealthSubCommand command = new ClusterHealthSubCommand();

        Assert.assertEquals("clusterHealth", command.commandName());
        Assert.assertTrue(command.commandDesc().contains("NameServer"));
    }

    @Test
    public void testBuildOptionsContainsScriptableControls() {
        ClusterHealthSubCommand command = new ClusterHealthSubCommand();
        Options options = command.buildCommandlineOptions(new Options());

        Assert.assertNotNull(options.getOption("brokerAddr"));
        Assert.assertNotNull(options.getOption("clusterName"));
        Assert.assertNotNull(options.getOption("namesrvOnly"));
        Assert.assertNotNull(options.getOption("mastersOnly"));
        Assert.assertNotNull(options.getOption("requireActive"));
        Assert.assertNotNull(options.getOption("timeoutMillis"));
        Assert.assertNotNull(options.getOption("parallelism"));
        Assert.assertNotNull(options.getOption("format"));
    }

    @Test
    public void testParseAllRequestOptions() throws Exception {
        ClusterHealthSubCommand command = new ClusterHealthSubCommand();
        CommandLine commandLine = parse(command,
            "-c", "production", "-m", "-a", "-t", "4500", "-p", "8", "-f", "json");

        ClusterHealthRequest request = command.request(commandLine);

        Assert.assertEquals("production", request.getClusterName());
        Assert.assertTrue(request.isMastersOnly());
        Assert.assertTrue(request.isRequireActive());
        Assert.assertEquals(4500, request.getTimeoutMillis());
        Assert.assertEquals(8, request.getParallelism());
        Assert.assertEquals(ClusterHealthSubCommand.OutputFormat.JSON, command.outputFormat(commandLine));
    }

    @Test
    public void testDefaultOutputIsText() throws Exception {
        ClusterHealthSubCommand command = new ClusterHealthSubCommand();
        CommandLine commandLine = parse(command, "-s");

        Assert.assertEquals(ClusterHealthSubCommand.OutputFormat.TEXT, command.outputFormat(commandLine));
    }

    @Test
    public void testOutputFormatIsCaseInsensitive() throws Exception {
        ClusterHealthSubCommand command = new ClusterHealthSubCommand();
        CommandLine commandLine = parse(command, "-s", "-f", "JSON");

        Assert.assertEquals(ClusterHealthSubCommand.OutputFormat.JSON, command.outputFormat(commandLine));
    }

    @Test
    public void testRejectUnknownOutputFormat() throws Exception {
        ClusterHealthSubCommand command = new ClusterHealthSubCommand();
        CommandLine commandLine = parse(command, "-s", "-f", "yaml");

        try {
            command.outputFormat(commandLine);
            Assert.fail("Expected format validation to fail");
        } catch (IllegalArgumentException e) {
            Assert.assertTrue(e.getMessage().contains("text or json"));
        }
    }

    @Test
    public void testRejectNonNumericTimeout() throws Exception {
        ClusterHealthSubCommand command = new ClusterHealthSubCommand();
        CommandLine commandLine = parse(command, "-s", "-t", "soon");

        try {
            command.request(commandLine);
            Assert.fail("Expected timeout validation to fail");
        } catch (IllegalArgumentException e) {
            Assert.assertTrue(e.getMessage().contains("timeoutMillis must be an integer"));
        }
    }

    @Test
    public void testRejectNonNumericParallelism() throws Exception {
        ClusterHealthSubCommand command = new ClusterHealthSubCommand();
        CommandLine commandLine = parse(command, "-s", "-p", "many");

        try {
            command.request(commandLine);
            Assert.fail("Expected parallelism validation to fail");
        } catch (IllegalArgumentException e) {
            Assert.assertTrue(e.getMessage().contains("parallelism must be an integer"));
        }
    }

    @Test
    public void testRunStartsAndStopsAdminClient() throws Exception {
        StubAdminExt adminExt = new StubAdminExt();
        adminExt.setBrokerResponse(runtime(true));
        final long[] factoryTimeout = new long[1];
        ClusterHealthSubCommand command = new ClusterHealthSubCommand((hook, timeout) -> {
            factoryTimeout[0] = timeout;
            return adminExt;
        });
        CommandLine commandLine = parse(command, "-b", "127.0.0.1:10911", "-t", "1200");

        ClusterHealthReport report = command.run(commandLine, null);

        Assert.assertTrue(report.isHealthy());
        Assert.assertTrue(adminExt.isStarted());
        Assert.assertTrue(adminExt.isShutdown());
        Assert.assertEquals(1200, factoryTimeout[0]);
        Assert.assertEquals("127.0.0.1:10911", adminExt.getLastBrokerAddress());
    }

    @Test
    public void testRunStopsAdminClientWhenProbeFails() throws Exception {
        StubAdminExt adminExt = new StubAdminExt();
        adminExt.setBrokerFailure(new IllegalStateException("offline"));
        ClusterHealthSubCommand command = new ClusterHealthSubCommand((hook, timeout) -> adminExt);
        CommandLine commandLine = parse(command, "-b", "127.0.0.1:10911");

        ClusterHealthReport report = command.run(commandLine, null);

        Assert.assertFalse(report.isHealthy());
        Assert.assertTrue(adminExt.isShutdown());
    }

    @Test
    public void testPrintTextReport() {
        ClusterHealthSubCommand command = new ClusterHealthSubCommand();
        ClusterHealthReport report = healthyReport();
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();

        command.printReport(report, ClusterHealthSubCommand.OutputFormat.TEXT, new PrintStream(bytes));

        Assert.assertTrue(bytes.toString().contains("STATUS       HEALTHY"));
    }

    @Test
    public void testPrintJsonReport() {
        ClusterHealthSubCommand command = new ClusterHealthSubCommand();
        ClusterHealthReport report = healthyReport();
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();

        command.printReport(report, ClusterHealthSubCommand.OutputFormat.JSON, new PrintStream(bytes));

        Assert.assertTrue(bytes.toString().contains("\"status\":\"HEALTHY\""));
    }

    private static CommandLine parse(ClusterHealthSubCommand command, String... arguments) throws Exception {
        Options options = ServerUtil.buildCommandlineOptions(new Options());
        options = command.buildCommandlineOptions(options);
        return new DefaultParser().parse(options, arguments);
    }

    private static ClusterHealthReport healthyReport() {
        ClusterHealthReport report = new ClusterHealthReport();
        report.setTarget("nameserver");
        report.setNameServerStatus(ClusterHealthReport.NameServerStatus.HEALTHY);
        report.setBrokers(java.util.Collections.emptyList());
        report.complete();
        return report;
    }

    private static KVTable runtime(boolean active) {
        KVTable table = new KVTable();
        HashMap<String, String> values = new HashMap<>();
        values.put("brokerActive", Boolean.toString(active));
        values.put("brokerVersionDesc", "V5_5_0");
        table.setTable(values);
        return table;
    }

    private static class StubAdminExt extends DefaultMQAdminExt {
        private boolean started;
        private boolean shutdown;
        private KVTable brokerResponse;
        private RuntimeException brokerFailure;
        private String lastBrokerAddress;

        @Override
        public void start() {
            started = true;
        }

        @Override
        public void shutdown() {
            shutdown = true;
        }

        @Override
        public ClusterInfo examineBrokerClusterInfo() {
            ClusterInfo clusterInfo = new ClusterInfo();
            clusterInfo.setBrokerAddrTable(new HashMap<>());
            clusterInfo.setClusterAddrTable(new HashMap<>());
            return clusterInfo;
        }

        @Override
        public KVTable fetchBrokerRuntimeStats(String brokerAddr) {
            lastBrokerAddress = brokerAddr;
            if (brokerFailure != null) {
                throw brokerFailure;
            }
            return brokerResponse;
        }

        boolean isStarted() {
            return started;
        }

        boolean isShutdown() {
            return shutdown;
        }

        void setBrokerResponse(KVTable brokerResponse) {
            this.brokerResponse = brokerResponse;
        }

        void setBrokerFailure(RuntimeException brokerFailure) {
            this.brokerFailure = brokerFailure;
        }

        String getLastBrokerAddress() {
            return lastBrokerAddress;
        }
    }
}
