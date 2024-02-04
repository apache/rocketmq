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
package org.apache.rocketmq.tools.command.broker;

import java.io.UnsupportedEncodingException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.exception.RemotingConnectException;
import org.apache.rocketmq.remoting.exception.RemotingSendRequestException;
import org.apache.rocketmq.remoting.exception.RemotingTimeoutException;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.apache.rocketmq.tools.admin.MQAdminExt;
import org.apache.rocketmq.tools.command.CommandUtil;
import org.apache.rocketmq.tools.command.SubCommand;
import org.apache.rocketmq.tools.command.SubCommandException;

public class GetColdDataFlowCtrInfoSubCommand implements SubCommand {
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    @Override
    public String commandName() {
        return "getColdDataFlowCtrInfo";
    }

    @Override
    public String commandDesc() {
        return "get cold data flow ctr info";
    }

    @Override
    public Options buildCommandlineOptions(final Options options) {
        Option opt = new Option("b", "brokerAddr", true, "get from which broker");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("c", "clusterName", true, "get from which cluster");
        opt.setRequired(false);
        options.addOption(opt);

        return options;
    }

    @Override
    public void execute(final CommandLine commandLine, final Options options, final RPCHook rpcHook)
        throws SubCommandException {
        DefaultMQAdminExt defaultMQAdminExt = new DefaultMQAdminExt(rpcHook);
        defaultMQAdminExt.setInstanceName(Long.toString(System.currentTimeMillis()));

        try {
            if (commandLine.hasOption('b')) {
                String brokerAddr = commandLine.getOptionValue('b').trim();
                defaultMQAdminExt.start();
                getAndPrint(defaultMQAdminExt, String.format("============%s============\n", brokerAddr), brokerAddr);
            } else if (commandLine.hasOption('c')) {
                String clusterName = commandLine.getOptionValue('c').trim();
                defaultMQAdminExt.start();
                Map<String, List<String>> masterAndSlaveMap = CommandUtil.fetchMasterAndSlaveDistinguish(defaultMQAdminExt, clusterName);
                for (String masterAddr : masterAndSlaveMap.keySet()) {
                    getAndPrint(defaultMQAdminExt, String.format("============Master: %s============\n", masterAddr), masterAddr);
                    for (String slaveAddr : masterAndSlaveMap.get(masterAddr)) {
                        getAndPrint(defaultMQAdminExt, String.format("============My Master: %s=====Slave: %s============\n", masterAddr, slaveAddr), slaveAddr);
                    }
                }
            }
        } catch (Exception e) {
            throw new SubCommandException(this.getClass().getSimpleName() + " command failed", e);
        } finally {
            defaultMQAdminExt.shutdown();
        }
    }

    protected void getAndPrint(final MQAdminExt defaultMQAdminExt, final String printPrefix, final String addr)
        throws InterruptedException, RemotingConnectException,
        UnsupportedEncodingException, RemotingTimeoutException,
        MQBrokerException, RemotingSendRequestException {

        System.out.print(" " + printPrefix);
        String rstStr = defaultMQAdminExt.getColdDataFlowCtrInfo(addr);
        if (rstStr == null) {
            System.out.printf("Broker[%s] has no cold ctr table !\n", addr);
            return;
        }
        JSONObject jsonObject = JSON.parseObject(rstStr);
        Map<String, JSONObject> runtimeTable = (Map<String, JSONObject>)jsonObject.get("runtimeTable");
        runtimeTable.entrySet().stream().forEach(i -> {
            JSONObject value = i.getValue();
            Date lastColdReadTimeMillsDate = new Date(Long.parseLong(String.valueOf(value.get("lastColdReadTimeMills"))));
            value.put("lastColdReadTimeFormat", sdf.format(lastColdReadTimeMillsDate));
            value.remove("lastColdReadTimeMills");

            Date createTimeMillsDate = new Date(Long.parseLong(String.valueOf(value.get("createTimeMills"))));
            value.put("createTimeFormat", sdf.format(createTimeMillsDate));
            value.remove("createTimeMills");
        });

        String formatStr = JSON.toJSONString(jsonObject, true);
        System.out.printf(formatStr);
        System.out.printf("%n");
    }

}
