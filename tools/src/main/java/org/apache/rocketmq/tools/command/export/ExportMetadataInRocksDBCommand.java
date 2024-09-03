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

import com.alibaba.fastjson.JSONObject;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.config.ConfigRocksDBStorage;
import org.apache.rocketmq.common.utils.DataConverter;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.tools.command.SubCommand;
import org.apache.rocketmq.tools.command.SubCommandException;
import org.rocksdb.RocksIterator;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;

public class ExportMetadataInRocksDBCommand implements SubCommand {
    private static final String TOPICS_JSON_CONFIG = "topics";
    private static final String SUBSCRIPTION_GROUP_JSON_CONFIG = "subscriptionGroups";

    @Override
    public String commandName() {
        return "exportMetadataInRocksDB";
    }

    @Override
    public String commandDesc() {
        return "export RocksDB kv config (topics/subscriptionGroups)";
    }

    @Override
    public Options buildCommandlineOptions(Options options) {
        Option pathOption = new Option("p", "path", true,
            "Absolute path for the metadata directory");
        pathOption.setRequired(true);
        options.addOption(pathOption);

        Option configTypeOption = new Option("t", "configType", true, "Name of kv config, e.g. " +
            "topics/subscriptionGroups");
        configTypeOption.setRequired(true);
        options.addOption(configTypeOption);

        Option jsonEnableOption = new Option("j", "jsonEnable", true,
            "Json format enable, Default: false");
        jsonEnableOption.setRequired(false);
        options.addOption(jsonEnableOption);

        return options;
    }

    @Override
    public void execute(CommandLine commandLine, Options options, RPCHook rpcHook) throws SubCommandException {
        String path = commandLine.getOptionValue("path").trim();
        if (StringUtils.isEmpty(path) || !UtilAll.isPathExists(path)) {
            System.out.print("RocksDB path is invalid.\n");
            return;
        }

        String configType = commandLine.getOptionValue("configType").trim().toLowerCase();
        path += "/" + configType;

        boolean jsonEnable = false;
        if (commandLine.hasOption("jsonEnable")) {
            jsonEnable = Boolean.parseBoolean(commandLine.getOptionValue("jsonEnable").trim());
        }


        ConfigRocksDBStorage kvStore = new ConfigRocksDBStorage(path, true /* readOnly */);
        if (!kvStore.start()) {
            System.out.printf("RocksDB load error, path=%s\n" , path);
            return;
        }

        try {
            if (TOPICS_JSON_CONFIG.equalsIgnoreCase(configType) || SUBSCRIPTION_GROUP_JSON_CONFIG.equalsIgnoreCase(configType)) {
                handleExportMetadata(kvStore, configType, jsonEnable);
            } else {
                System.out.printf("Invalid config type=%s, Options: topics,subscriptionGroups\n", configType);
            }
        } finally {
            kvStore.shutdown();
        }
    }

    private static void handleExportMetadata(ConfigRocksDBStorage kvStore, String configType, boolean jsonEnable) {
        if (jsonEnable) {
            final Map<String, JSONObject> jsonConfig = new HashMap<>();
            final Map<String, JSONObject> configTable = new HashMap<>();
            iterateKvStore(kvStore, (key, value) -> {
                    final String configKey = new String(key, DataConverter.CHARSET_UTF8);
                    final String configValue = new String(value, DataConverter.CHARSET_UTF8);
                    final JSONObject jsonObject = JSONObject.parseObject(configValue);
                    configTable.put(configKey, jsonObject);
                }
            );

            jsonConfig.put(configType.equalsIgnoreCase(TOPICS_JSON_CONFIG) ? "topicConfigTable" : "subscriptionGroupTable",
                (JSONObject) JSONObject.toJSON(configTable));
            final String jsonConfigStr = JSONObject.toJSONString(jsonConfig, true);
            System.out.print(jsonConfigStr + "\n");
        } else {
            AtomicLong count = new AtomicLong(0);
            iterateKvStore(kvStore, (key, value) -> {
                final String configKey = new String(key, DataConverter.CHARSET_UTF8);
                final String configValue = new String(value, DataConverter.CHARSET_UTF8);
                System.out.printf("%d, Key: %s, Value: %s%n", count.incrementAndGet(), configKey, configValue);
            });
        }
    }

    private static void iterateKvStore(ConfigRocksDBStorage kvStore, BiConsumer<byte[], byte[]> biConsumer) {
        try (RocksIterator iterator = kvStore.iterator()) {
            iterator.seekToFirst();
            for (iterator.seekToFirst(); iterator.isValid(); iterator.next()) {
                biConsumer.accept(iterator.key(), iterator.value());
            }
        }
    }
}
