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
package org.apache.rocketmq.tools.command.export;

import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.apache.rocketmq.tools.command.CommandUtil;
import org.apache.rocketmq.tools.command.MQAdminStartup;
import org.apache.rocketmq.tools.command.SubCommand;
import org.apache.rocketmq.tools.command.SubCommandException;

public class ExportNamesrvAndBrokerConfigCommand implements SubCommand {
    @Override
    public String commandName() {
        return "exportNamesrvAndBrokerConfig";
    }

    @Override
    public String commandDesc() {
        return "export namesrvAndBrokerConfig.properties";
    }

    @Override
    public Options buildCommandlineOptions(Options options) {
        Option opt = new Option("c", "clusterName", true, "choose a cluster to export");
        opt.setRequired(true);
        options.addOption(opt);

        opt = new Option("f", "filePath", true,
            "export namesrvAndBrokerConfig.properties path | default /tmp/rocketmq/config");
        opt.setRequired(false);
        options.addOption(opt);
        return options;
    }

    @Override
    public void execute(CommandLine commandLine, Options options, RPCHook rpcHook)
        throws SubCommandException {
        DefaultMQAdminExt defaultMQAdminExt = new DefaultMQAdminExt(rpcHook);
        defaultMQAdminExt.setInstanceName(Long.toString(System.currentTimeMillis()));

        try {
            String clusterName = commandLine.getOptionValue('c').trim();
            String filePath = !commandLine.hasOption('f') ? "/tmp/rocketmq/config" : commandLine.getOptionValue('f')
                .trim();

            defaultMQAdminExt.start();
            StringBuilder configStr = new StringBuilder();
            // name servers
            Map<String, Properties> nameServerConfigs = defaultMQAdminExt.getNameServerConfig(null);
            configStr.append("============namesrv config============\n");
            for (String server : nameServerConfigs.keySet()) {
                configStr.append(String.format("============%s============\n", server));
                for (Object key : nameServerConfigs.get(server).keySet()) {
                    configStr.append(String.format("%-50s=  %s\n", key, nameServerConfigs.get(server).get(key)));
                }
            }

            configStr.append("\n\n");

            //broker
            configStr.append("============broker config============\n");
            Map<String, List<String>> masterAndSlaveMap
                = CommandUtil.fetchMasterAndSlaveDistinguish(defaultMQAdminExt, clusterName);
            for (String masterAddr : masterAndSlaveMap.keySet()) {
                configStr.append(String.format("============Master: %s============\n", masterAddr));
                Properties masterProperties = defaultMQAdminExt.getBrokerConfig(masterAddr);
                for (Object key : masterProperties.keySet()) {
                    configStr.append(String.format("%-50s=  %s\n", key, masterProperties.get(key)));
                }
                for (String slaveAddr : masterAndSlaveMap.get(masterAddr)) {

                    configStr.append(
                        String.format("============My Master: %s=====Slave: %s============\n", masterAddr, slaveAddr));
                    Properties slaveProperties = defaultMQAdminExt.getBrokerConfig(slaveAddr);
                    for (Object key : slaveProperties.keySet()) {
                        configStr.append(String.format("%-50s=  %s\n", key, slaveProperties.get(key)));
                    }
                }
                configStr.append("\n");
            }

            String path = filePath + "/namesrvAndBrokerConfig.properties";
            CommandUtil.write2File(configStr.toString(), path);
            System.out.printf("export %s success", path);
        } catch (Exception e) {
            throw new SubCommandException(this.getClass().getSimpleName() + " command failed", e);
        } finally {
            defaultMQAdminExt.shutdown();
        }

    }

    public static void main(String[] args) {
        System.setProperty(MixAll.NAMESRV_ADDR_PROPERTY, "127.0.0.1:9876");
        MQAdminStartup.main(
            new String[] {new ExportNamesrvAndBrokerConfigCommand().commandName(), "-c", "DefaultCluster"});
    }
}
