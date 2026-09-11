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

import java.io.PrintStream;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.apache.rocketmq.tools.command.SubCommand;
import org.apache.rocketmq.tools.command.SubCommandException;

public class ClusterHealthSubCommand implements SubCommand {
    public enum OutputFormat {
        TEXT,
        JSON
    }

    interface AdminFactory {
        DefaultMQAdminExt create(RPCHook rpcHook, long timeoutMillis);
    }

    private final AdminFactory adminFactory;

    public ClusterHealthSubCommand() {
        this(DefaultMQAdminExt::new);
    }

    ClusterHealthSubCommand(AdminFactory adminFactory) {
        this.adminFactory = adminFactory;
    }

    @Override
    public String commandName() {
        return "clusterHealth";
    }

    @Override
    public String commandDesc() {
        return "Check NameServer and broker health without producing test messages.";
    }

    @Override
    public Options buildCommandlineOptions(Options options) {
        Option option = new Option("b", "brokerAddr", true,
            "Check one broker directly and skip NameServer discovery");
        option.setRequired(false);
        options.addOption(option);

        option = new Option("c", "clusterName", true, "Check brokers in this cluster only");
        option.setRequired(false);
        options.addOption(option);

        option = new Option("s", "namesrvOnly", false, "Only check the NameServer metadata RPC");
        option.setRequired(false);
        options.addOption(option);

        option = new Option("m", "mastersOnly", false, "Check master broker addresses only");
        option.setRequired(false);
        options.addOption(option);

        option = new Option("a", "requireActive", false,
            "Also require brokerActive=true (normally appropriate for writable masters)");
        option.setRequired(false);
        options.addOption(option);

        option = new Option("t", "timeoutMillis", true,
            "RPC and overall broker-check deadline in milliseconds; default 3000");
        option.setRequired(false);
        options.addOption(option);

        option = new Option("p", "parallelism", true,
            "Maximum concurrent broker probes, from 1 to 64; default 4");
        option.setRequired(false);
        options.addOption(option);

        option = new Option("f", "format", true, "Output format: text or json; default text");
        option.setRequired(false);
        options.addOption(option);
        return options;
    }

    @Override
    public void execute(CommandLine commandLine, Options options, RPCHook rpcHook) throws SubCommandException {
        try {
            OutputFormat format = outputFormat(commandLine);
            ClusterHealthReport report = run(commandLine, rpcHook);
            printReport(report, format, System.out);
            if (!report.isHealthy()) {
                throw new SubCommandException("Cluster health check reported UNHEALTHY");
            }
        } catch (SubCommandException e) {
            throw e;
        } catch (Exception e) {
            throw new SubCommandException(getClass().getSimpleName() + " command failed", e);
        }
    }

    public ClusterHealthReport run(CommandLine commandLine, RPCHook rpcHook) throws Exception {
        ClusterHealthRequest request = request(commandLine);
        DefaultMQAdminExt adminExt = adminFactory.create(rpcHook, request.getTimeoutMillis());
        adminExt.setInstanceName("clusterHealth-" + System.currentTimeMillis());
        try {
            adminExt.start();
            ClusterHealthChecker checker = new ClusterHealthChecker(
                new ClusterHealthChecker.DefaultAdminAccess(adminExt));
            return checker.check(request);
        } finally {
            adminExt.shutdown();
        }
    }

    ClusterHealthRequest request(CommandLine commandLine) {
        ClusterHealthRequest request = new ClusterHealthRequest();
        if (commandLine.hasOption('b')) {
            request.setBrokerAddr(commandLine.getOptionValue('b'));
        }
        if (commandLine.hasOption('c')) {
            request.setClusterName(commandLine.getOptionValue('c'));
        }
        request.setNamesrvOnly(commandLine.hasOption('s'));
        request.setMastersOnly(commandLine.hasOption('m'));
        request.setRequireActive(commandLine.hasOption('a'));
        if (commandLine.hasOption('t')) {
            request.setTimeoutMillis(parseLong(commandLine.getOptionValue('t'), "timeoutMillis"));
        }
        if (commandLine.hasOption('p')) {
            request.setParallelism(parseInt(commandLine.getOptionValue('p'), "parallelism"));
        }
        request.validate();
        return request;
    }

    public OutputFormat outputFormat(CommandLine commandLine) {
        if (!commandLine.hasOption('f')) {
            return OutputFormat.TEXT;
        }
        String value = commandLine.getOptionValue('f').trim();
        if ("text".equalsIgnoreCase(value)) {
            return OutputFormat.TEXT;
        }
        if ("json".equalsIgnoreCase(value)) {
            return OutputFormat.JSON;
        }
        throw new IllegalArgumentException("format must be text or json");
    }

    public void printReport(ClusterHealthReport report, OutputFormat format, PrintStream out) {
        if (OutputFormat.JSON.equals(format)) {
            out.println(report.toJson());
        } else {
            out.print(report.toText());
        }
    }

    private static long parseLong(String value, String optionName) {
        try {
            return Long.parseLong(value.trim());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(optionName + " must be an integer", e);
        }
    }

    private static int parseInt(String value, String optionName) {
        try {
            return Integer.parseInt(value.trim());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(optionName + " must be an integer", e);
        }
    }
}
