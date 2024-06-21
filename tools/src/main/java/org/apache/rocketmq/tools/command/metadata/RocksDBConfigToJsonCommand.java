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
package org.apache.rocketmq.tools.command.metadata;

import com.alibaba.fastjson.JSONObject;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.config.RocksDBConfigManager;
import org.apache.rocketmq.common.utils.DataConverter;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.tools.command.SubCommand;
import org.apache.rocketmq.tools.command.SubCommandException;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

public class RocksDBConfigToJsonCommand implements SubCommand {
    private static final String TOPICS_JSON_CONFIG = "topics";
    private static final String SUBSCRIPTION_GROUP_JSON_CONFIG = "subscriptionGroups";

    @Override
    public String commandName() {
        return "rocksDBConfigToJson";
    }

    @Override
    public String commandDesc() {
        return "Convert RocksDB kv config (topics/subscriptionGroups) to json";
    }

    @Override
    public Options buildCommandlineOptions(Options options) {
        Option pathOption = new Option("p", "path", true,
                "Absolute path to the metadata directory");
        pathOption.setRequired(true);
        options.addOption(pathOption);

        Option configTypeOption = new Option("t", "configType", true, "Name of kv config, e.g. " +
                "topics/subscriptionGroups");
        configTypeOption.setRequired(true);
        options.addOption(configTypeOption);

        return options;
    }

    @Override
    public void execute(CommandLine commandLine, Options options, RPCHook rpcHook) throws SubCommandException {
        String path = commandLine.getOptionValue("path").trim();
        if (StringUtils.isEmpty(path) || !new File(path).exists()) {
            System.out.print("Rocksdb path is invalid.\n");
            return;
        }

        String configType = commandLine.getOptionValue("configType").trim().toLowerCase();

        final long memTableFlushInterval = 60 * 60 * 1000L;
        RocksDBConfigManager kvConfigManager = new RocksDBConfigManager(memTableFlushInterval);
        try {
            if (TOPICS_JSON_CONFIG.toLowerCase().equals(configType)) {
                // for topics.json
                final Map<String, JSONObject> topicsJsonConfig = new HashMap<>();
                final Map<String, JSONObject> topicConfigTable = new HashMap<>();
                boolean isLoad = kvConfigManager.load(path, (key, value) -> {
                    final String topic = new String(key, DataConverter.CHARSET_UTF8);
                    final String topicConfig = new String(value, DataConverter.CHARSET_UTF8);
                    final JSONObject jsonObject = JSONObject.parseObject(topicConfig);
                    topicConfigTable.put(topic, jsonObject);
                });

                if (isLoad) {
                    topicsJsonConfig.put("topicConfigTable", (JSONObject) JSONObject.toJSON(topicConfigTable));
                    final String topicsJsonStr = JSONObject.toJSONString(topicsJsonConfig, true);
                    System.out.print(topicsJsonStr + "\n");
                    return;
                }
            }
            if (SUBSCRIPTION_GROUP_JSON_CONFIG.toLowerCase().equals(configType)) {
                // for subscriptionGroup.json
                final Map<String, JSONObject> subscriptionGroupJsonConfig = new HashMap<>();
                final Map<String, JSONObject> subscriptionGroupTable = new HashMap<>();
                boolean isLoad = kvConfigManager.load(path, (key, value) -> {
                    final String subscriptionGroup = new String(key, DataConverter.CHARSET_UTF8);
                    final String subscriptionGroupConfig = new String(value, DataConverter.CHARSET_UTF8);
                    final JSONObject jsonObject = JSONObject.parseObject(subscriptionGroupConfig);
                    subscriptionGroupTable.put(subscriptionGroup, jsonObject);
                });

                if (isLoad) {
                    subscriptionGroupJsonConfig.put("subscriptionGroupTable",
                            (JSONObject) JSONObject.toJSON(subscriptionGroupTable));
                    final String subscriptionGroupJsonStr = JSONObject.toJSONString(subscriptionGroupJsonConfig, true);
                    System.out.print(subscriptionGroupJsonStr + "\n");
                    return;
                }
            }
            System.out.print("Config type was not recognized, configType=" + configType + "\n");
        } finally {
            kvConfigManager.stop();
        }
    }
}
