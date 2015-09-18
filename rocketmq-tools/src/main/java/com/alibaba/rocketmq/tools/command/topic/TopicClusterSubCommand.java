/**
 * Copyright (C) 2010-2013 Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.rocketmq.tools.command.topic;

import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.common.protocol.body.ClusterInfo;
import com.alibaba.rocketmq.common.protocol.route.BrokerData;
import com.alibaba.rocketmq.common.protocol.route.TopicRouteData;
import com.alibaba.rocketmq.remoting.RPCHook;
import com.alibaba.rocketmq.remoting.exception.RemotingException;
import com.alibaba.rocketmq.tools.admin.DefaultMQAdminExt;
import com.alibaba.rocketmq.tools.command.CommandUtil;
import com.alibaba.rocketmq.tools.command.SubCommand;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;


/**
 * 查看Topic所在的集群信息
 * 
 * @author zhouli
 * @since 2015-9-17
 */
public class TopicClusterSubCommand implements SubCommand {

    @Override
    public String commandName() {
        return "topicClusterList";
    }


    @Override
    public String commandDesc() {
        return "Fetch all cluster list for topic";
    }


    @Override
    public Options buildCommandlineOptions(Options options) {
        Option opt = new Option("t", "topic", true, "topic name");
        opt.setRequired(true);
        options.addOption(opt);
        return options;
    }

    @Override
    public void execute(final CommandLine commandLine, final Options options, RPCHook rpcHook) {
        DefaultMQAdminExt defaultMQAdminExt = new DefaultMQAdminExt(rpcHook);
        defaultMQAdminExt.setInstanceName(Long.toString(System.currentTimeMillis()));
        String topic = commandLine.getOptionValue('t').trim();
        try {
            defaultMQAdminExt.start();
            ClusterInfo clusterInfo = defaultMQAdminExt.examineBrokerClusterInfo();

            TopicRouteData topicRouteData = defaultMQAdminExt.examineTopicRouteInfo(topic);

            BrokerData brokerData = topicRouteData.getBrokerDatas().get(0);

            String brokerName = brokerData.getBrokerName();

            Iterator<Map.Entry<String, Set<String>>> it = clusterInfo.getClusterAddrTable().entrySet().iterator();
            while (it.hasNext()) {
                Map.Entry<String, Set<String>> next = it.next();
                if (next.getValue().contains(brokerName)) {
                    System.out.println(next.getKey());
                }
            }
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        finally {
            defaultMQAdminExt.shutdown();
        }
    }
}
