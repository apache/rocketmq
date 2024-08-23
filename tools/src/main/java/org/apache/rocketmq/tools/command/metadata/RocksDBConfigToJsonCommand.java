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

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.config.ConfigRocksDBStorage;
import org.apache.rocketmq.common.utils.DataConverter;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.tools.command.SubCommand;
import org.apache.rocketmq.tools.command.SubCommandException;
import org.rocksdb.RocksIterator;

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
        Option pathOption = new Option("p", "configPath", true,
                "Absolute path to the metadata config directory");
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
        String path = commandLine.getOptionValue("configPath").trim();
        if (StringUtils.isEmpty(path) || !new File(path).exists()) {
            System.out.print("Rocksdb path is invalid.\n");
            return;
        }

        String configType = commandLine.getOptionValue("configType").trim().toLowerCase();
        if (!path.endsWith("/")) {
            path += "/";
        }
        path += configType;

        ConfigRocksDBStorage configRocksDBStorage = new ConfigRocksDBStorage(path, true);
        configRocksDBStorage.start();
        RocksIterator iterator = configRocksDBStorage.iterator();

        try {
            final Map<String, JSONObject> configMap = new HashMap<>();
            final Map<String, JSONObject> configTable = new HashMap<>();
            iterator.seekToFirst();
            while (iterator.isValid()) {
                final byte[] key = iterator.key();
                final byte[] value = iterator.value();
                final String name = new String(key, DataConverter.CHARSET_UTF8);
                final String config = new String(value, DataConverter.CHARSET_UTF8);
                final JSONObject jsonObject = JSONObject.parseObject(config);
                configTable.put(name, jsonObject);
                iterator.next();
            }
            byte[] kvDataVersion = configRocksDBStorage.getKvDataVersion();
            configMap.put("dataVersion",
                    JSONObject.parseObject(new String(kvDataVersion, DataConverter.CHARSET_UTF8)));

            if (TOPICS_JSON_CONFIG.toLowerCase().equals(configType)) {
                configMap.put("topicConfigTable", JSON.parseObject(JSONObject.toJSONString(configTable)));
            }
            if (SUBSCRIPTION_GROUP_JSON_CONFIG.toLowerCase().equals(configType)) {
                configMap.put("subscriptionGroupTable", JSON.parseObject(JSONObject.toJSONString(configTable)));
            }
            System.out.print(JSONObject.toJSONString(configMap, true) + "\n");
        } catch (Exception e) {
            System.out.print("Error occurred while converting RocksDB kv config to json, " + "configType=" + configType + ", " + e.getMessage() + "\n");
        } finally {
            configRocksDBStorage.shutdown();
        }
    }
}