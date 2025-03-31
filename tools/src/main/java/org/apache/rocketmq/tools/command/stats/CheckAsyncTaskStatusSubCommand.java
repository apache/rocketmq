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

package org.apache.rocketmq.tools.command.stats;

import java.util.Set;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.collections.CollectionUtils;
import org.apache.rocketmq.common.AsyncTask;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.protocol.body.ClusterInfo;
import org.apache.rocketmq.remoting.protocol.header.CheckAsyncTaskStatusRequestHeader;
import org.apache.rocketmq.remoting.protocol.route.BrokerData;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.apache.rocketmq.tools.command.SubCommand;

import java.util.List;
import java.util.Map;

public class CheckAsyncTaskStatusSubCommand implements SubCommand {

    private DefaultMQAdminExt defaultMQAdminExt;

    private static final int DEFAULT_MAX_TASKS = 20;

    private DefaultMQAdminExt createDefaultMQAdminExt(RPCHook rpcHook) {
        if (defaultMQAdminExt == null) {
            defaultMQAdminExt = new DefaultMQAdminExt(rpcHook);
            defaultMQAdminExt.setInstanceName(Long.toString(System.currentTimeMillis()));
        } else {
            return defaultMQAdminExt;
        }
        return defaultMQAdminExt;
    }

    @Override
    public String commandName() {
        return "checkAsyncTaskStatus";
    }

    @Override
    public String commandDesc() {
        return "Check the status of an asynchronous task by task name.";
    }

    @Override
    public Options buildCommandlineOptions(Options options) {
        Option opt = new Option("t", "taskName", true, "The name of the asynchronous task");
        opt.setRequired(true);
        options.addOption(opt);

        opt = new Option("i", "taskId", true, "The id of the asynchronous task");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("b", "brokerAddr", true, "Check which broker");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("c", "clusterName", true, "Check which cluster");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("n", "nameSrvAddr", true, "Nameserver address");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("m", "maxLimit", true, "Maximum number of tasks to return");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("s", "taskStatus", true, "Filter tasks by status (0 for init, 1 for in progress, 2 for error, 3 for success.)");
        opt.setRequired(false);
        options.addOption(opt);

        return options;
    }

    @Override
    public void execute(CommandLine commandLine, Options options, RPCHook rpcHook) {
        defaultMQAdminExt = createDefaultMQAdminExt(rpcHook);

        String taskName = commandLine.hasOption('t') ? commandLine.getOptionValue('t').trim() : null;
        String taskId = commandLine.hasOption('i') ? commandLine.getOptionValue('i').trim() : null;
        if (taskName == null && taskId == null) {
            System.out.print("Either task name or task ID must be provided.");
            return;
        }
        String brokerAddr = commandLine.hasOption('b') ? commandLine.getOptionValue('b').trim() : null;
        String clusterName = commandLine.hasOption('c') ? commandLine.getOptionValue('c').trim() : null;
        String namesAddr = commandLine.hasOption('n') ? commandLine.getOptionValue('n').trim() : null;
        String maxLimitStr = commandLine.hasOption('m') ? commandLine.getOptionValue('m').trim() : null;
        int maxLimit = DEFAULT_MAX_TASKS;
        if (maxLimitStr != null && !maxLimitStr.isEmpty()) {
            try {
                maxLimit = Integer.parseInt(maxLimitStr);
            } catch (NumberFormatException e) {
                System.out.print("Illegal maxLimit parameter value");
                return;
            }
        }

        Integer taskStatus = commandLine.hasOption('s') ? Integer.parseInt(commandLine.getOptionValue('s').trim()) : null;

        try {
            defaultMQAdminExt.start();

            if (namesAddr != null) {
                defaultMQAdminExt.setNamesrvAddr(namesAddr);
            }

            ClusterInfo clusterInfo = defaultMQAdminExt.examineBrokerClusterInfo();
            if (clusterInfo == null) {
                System.out.print("Cluster info is empty.");
                return;
            }
            Map<String, BrokerData> brokerAddrTable = clusterInfo.getBrokerAddrTable();
            Map<String, Set<String>> clusterAddrTable = clusterInfo.getClusterAddrTable();

            if (brokerAddr != null) {
                // If brokerAddr is specified, check that broker first.
                if (brokerAddrTable == null || brokerAddrTable.isEmpty()) {
                    System.out.print("Broker address table is empty.");
                    return;
                }
                BrokerData brokerData = brokerAddrTable.values().stream()
                    .filter(b -> b.getBrokerAddrs().containsValue(brokerAddr))
                    .findFirst()
                    .orElse(null);

                if (brokerData != null) {
                    checkAsyncTaskStatusOnBroker(brokerAddr, taskName, taskId, brokerData.getBrokerName(), maxLimit, taskStatus);
                } else {
                    System.out.printf("Broker with address '%s' not found.%n", brokerAddr);
                }
            } else if (clusterName != null) {
                // If brokerAddr is not specified but clusterName is specified, check all brokers in the cluster.
                Set<String> brokerNames = clusterAddrTable.get(clusterName);
                if (brokerNames == null || brokerNames.isEmpty()) {
                    System.out.printf("Cluster '%s' not found or has no brokers.%n", clusterName);
                    return;
                }
                int finalMaxLimit = maxLimit;
                brokerNames.forEach(brokerName -> {
                    BrokerData brokerData = brokerAddrTable.get(brokerName);
                    if (brokerData != null) {
                        checkAsyncTaskStatusOnBroker(brokerData.selectBrokerAddr(), taskName, taskId, brokerName, finalMaxLimit, taskStatus);
                    }
                });
            } else {
                // If neither brokerAddr nor clusterName is specified, check all brokers.
                for (Map.Entry<String, BrokerData> entry : brokerAddrTable.entrySet()) {
                    String brokerName = entry.getKey();
                    String addr = entry.getValue().selectBrokerAddr();
                    checkAsyncTaskStatusOnBroker(addr, taskName, taskId, brokerName, maxLimit, taskStatus);
                }
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to execute " + this.getClass().getSimpleName() + " command", e);
        } finally {
            defaultMQAdminExt.shutdown();
        }
    }

    private void checkAsyncTaskStatusOnBroker(String brokerAddr, String taskName, String taskId, String brokerName, int maxLimit, Integer taskStatus) {
        try {
            CheckAsyncTaskStatusRequestHeader requestHeader = new CheckAsyncTaskStatusRequestHeader();
            requestHeader.setTaskName(taskName);
            requestHeader.setTaskId(taskId);
            requestHeader.setMaxLimit(maxLimit);
            requestHeader.setTaskStatus(taskStatus);

            List<AsyncTask> asyncTaskStatus = defaultMQAdminExt.checkAsyncTaskStatus(brokerAddr, requestHeader);

            if (CollectionUtils.isNotEmpty(asyncTaskStatus)) {
                for (AsyncTask task : asyncTaskStatus) {
                    System.out.printf(
                        "Task found for task name '%s' on broker %s: [Task ID: %s, Status: %s, Result: %s, Create Time: %s]%n",
                        taskName, brokerName, task.getTaskId(), AsyncTask.getDescFromStatus(task.getStatus()),
                        task.getResult(), task.getCreateTime().toString()
                    );
                }
            } else {
                System.out.printf("No tasks found for task name '%s' on broker %s.%n", taskName, brokerName);
            }
        } catch (Exception e) {
            System.out.printf("Failed to query task status for task name '%s' on broker %s: %s%n",
                taskName, brokerName, e.getMessage());
        }
    }
}
