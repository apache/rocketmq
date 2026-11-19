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
package org.apache.rocketmq.tools.command;

import java.io.PrintStream;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.rocketmq.acl.common.AclUtils;
import org.apache.rocketmq.common.MQVersion;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.srvutil.ServerUtil;
import org.apache.rocketmq.tools.command.cluster.ClusterHealthReport;
import org.apache.rocketmq.tools.command.cluster.ClusterHealthSubCommand;

public class MQHealthCheckStartup {
    public static final int EXIT_HEALTHY = 0;
    public static final int EXIT_UNHEALTHY = 1;
    public static final int EXIT_USAGE_OR_ERROR = 2;

    public static void main(String[] args) {
        int exitCode = main0(args, null, System.out, System.err);
        System.exit(exitCode);
    }

    static int main0(String[] args, RPCHook rpcHook, PrintStream out, PrintStream err) {
        System.setProperty(RemotingCommand.REMOTING_VERSION_KEY, Integer.toString(MQVersion.CURRENT_VERSION));
        ClusterHealthSubCommand command = new ClusterHealthSubCommand();
        Options options = ServerUtil.buildCommandlineOptions(new Options());
        options = command.buildCommandlineOptions(options);
        try {
            CommandLine commandLine = ServerUtil.parseCmdLine("mqhealthcheck", args, options, new DefaultParser());
            if (commandLine == null) {
                return EXIT_USAGE_OR_ERROR;
            }
            if (commandLine.hasOption('n')) {
                System.setProperty(MixAll.NAMESRV_ADDR_PROPERTY, commandLine.getOptionValue('n'));
            }
            RPCHook effectiveHook = rpcHook == null ? loadAclHook() : rpcHook;
            ClusterHealthSubCommand.OutputFormat format = command.outputFormat(commandLine);
            ClusterHealthReport report = command.run(commandLine, effectiveHook);
            command.printReport(report, format, out);
            return report.isHealthy() ? EXIT_HEALTHY : EXIT_UNHEALTHY;
        } catch (Exception e) {
            err.println("mqhealthcheck failed: " + conciseMessage(e));
            return EXIT_USAGE_OR_ERROR;
        }
    }

    private static RPCHook loadAclHook() {
        String home = MixAll.ROCKETMQ_HOME_DIR;
        if (home == null) {
            return null;
        }
        return AclUtils.getAclRPCHook(home + MixAll.ACL_CONF_TOOLS_FILE);
    }

    private static String conciseMessage(Throwable throwable) {
        Throwable root = throwable;
        while (root.getCause() != null && root.getCause() != root) {
            root = root.getCause();
        }
        String message = root.getMessage();
        return message == null || message.trim().isEmpty()
            ? root.getClass().getSimpleName() : root.getClass().getSimpleName() + ": " + message;
    }
}
