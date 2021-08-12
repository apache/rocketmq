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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import com.alibaba.fastjson.JSON;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.apache.rocketmq.tools.command.CommandUtil;
import org.apache.rocketmq.tools.command.SubCommand;
import org.apache.rocketmq.tools.command.SubCommandException;

public class ExportNamesrvAndBrokerConfigCommand implements SubCommand {
    @Override
    public String commandName() {
        return "exportNamesrvAndBrokerConfig";
    }

    @Override
    public String commandDesc() {
        return "export namesrvAndBrokerConfig.json";
    }

    @Override
    public Options buildCommandlineOptions(Options options) {
        Option opt = new Option("c", "clusterName", true, "choose a cluster to export");
        opt.setRequired(true);
        options.addOption(opt);

        opt = new Option("f", "filePath", true,
            "export namesrvAndBrokerConfig.json path | default /tmp/rocketmq/config");
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
            Map<String, Object> result = new HashMap<>();
            // name servers
            Map<String, Properties> nameServerConfigs = defaultMQAdminExt.getNameServerConfig(null);
            result.put("nameServerConfigs", nameServerConfigs);

            //broker
            Map<String, Map<String, Properties>> brokerConfigs = new HashMap<>();
            Map<String, List<String>> masterAndSlaveMap
                = CommandUtil.fetchMasterAndSlaveDistinguish(defaultMQAdminExt, clusterName);
            for (String masterAddr : masterAndSlaveMap.keySet()) {
                Map<String, Properties> map = new HashMap<>();
                Properties masterProperties = defaultMQAdminExt.getBrokerConfig(masterAddr);
                map.put(masterAddr, masterProperties);
                for (String slaveAddr : masterAndSlaveMap.get(masterAddr)) {
                    Properties slaveProperties = defaultMQAdminExt.getBrokerConfig(slaveAddr);
                    map.put(slaveAddr, slaveProperties);
                }
                brokerConfigs.put(masterProperties.getProperty("brokerName"), map);
            }

            result.put("brokerConfigs", brokerConfigs);

            String path = filePath + "/namesrvAndBrokerConfig.json";
            MixAll.string2FileNotSafe(JSON.toJSONString(result, true), path);
            System.out.printf("export %s success", path);
        } catch (Exception e) {
            throw new SubCommandException(this.getClass().getSimpleName() + " command failed", e);
        } finally {
            defaultMQAdminExt.shutdown();
        }
    }
}
