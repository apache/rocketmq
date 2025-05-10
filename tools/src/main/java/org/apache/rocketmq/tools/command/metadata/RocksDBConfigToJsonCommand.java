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

import com.alibaba.fastjson2.JSONObject;
import com.alibaba.fastjson2.JSONWriter;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.config.ConfigRocksDBStorage;
import org.apache.rocketmq.common.utils.DataConverter;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.protocol.body.ClusterInfo;
import org.apache.rocketmq.remoting.protocol.header.ExportRocksDBConfigToJsonRequestHeader;
import org.apache.rocketmq.remoting.protocol.route.BrokerData;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.apache.rocketmq.tools.command.SubCommand;
import org.apache.rocketmq.tools.command.SubCommandException;
import org.rocksdb.RocksIterator;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

public class RocksDBConfigToJsonCommand implements SubCommand {

    @Override
    public String commandName() {
        return "rocksDBConfigToJson";
    }

    @Override
    public String commandDesc() {
        return "Convert RocksDB kv config (topics/subscriptionGroups/consumerOffsets) to json. " +
            "[rpc mode] Use [-n, -c, -b, -t] to send Request to broker ( version >= 5.3.2 ) or [local mode] use [-p, -t, -j, -e] to load RocksDB. " +
            "If -e is provided, tools will export json file instead of std print";
    }

    @Override
    public Options buildCommandlineOptions(Options options) {
        Option configTypeOption = new Option("t", "configType", true, "Name of kv config, e.g. " +
            "topics/subscriptionGroups/consumerOffsets. Required in local mode and default all in rpc mode.");
        options.addOption(configTypeOption);

        // [local mode] options
        Option pathOption = new Option("p", "configPath", true,
            "[local mode] Absolute path to the metadata config directory");
        options.addOption(pathOption);

        Option exportPathOption = new Option("e", "exportFile", true,
            "[local mode] Absolute file path for exporting, auto backup existing file, not directory. If exportFile is provided, will export Json file and ignore [-j].");
        options.addOption(exportPathOption);

        Option jsonEnableOption = new Option("j", "jsonEnable", true,
            "[local mode] Json format enable, Default: true. If exportFile is provided, will export Json file and ignore [-j].");
        options.addOption(jsonEnableOption);

        // [rpc mode] options
        Option nameserverOption = new Option("n", "nameserverAddr", true,
            "[rpc mode] nameserverAddr. If nameserverAddr and clusterName are provided, will ignore [-p, -e, -j, -b] args");
        options.addOption(nameserverOption);

        Option clusterOption = new Option("c", "cluster", true,
            "[rpc mode] Cluster name. If nameserverAddr and clusterName are provided, will ignore [-p, -e, -j, -b] args");
        options.addOption(clusterOption);

        Option brokerAddrOption = new Option("b", "brokerAddr", true,
            "[rpc mode] Broker address. If brokerAddr is provided, will ignore [-p, -e, -j] args");
        options.addOption(brokerAddrOption);

        return options;
    }

    @Override
    public void execute(CommandLine commandLine, Options options, RPCHook rpcHook) throws SubCommandException {
        List<ExportRocksDBConfigToJsonRequestHeader.ConfigType> typeList = getConfigTypeList(commandLine);

        if (commandLine.hasOption("nameserverAddr")) {
            // [rpc mode] call all brokers in cluster to export to json file
            System.out.print("Use [rpc mode] call all brokers in cluster to export to json file \n");
            checkRequiredArgsProvided(commandLine, "rpc mode", "cluster");
            handleRpcMode(commandLine, rpcHook, typeList);
        } else if (commandLine.hasOption("brokerAddr")) {
            // [rpc mode] call broker to export to json file
            System.out.print("Use [rpc mode] call broker to export to json file \n");
            handleRpcMode(commandLine, rpcHook, typeList);
        } else if (commandLine.hasOption("configPath")) {
            // [local mode] load rocksdb to print or export file
            System.out.print("Use [local mode] load rocksdb to print or export file \n");
            checkRequiredArgsProvided(commandLine, "local mode", "configType");
            handleLocalMode(commandLine);
        } else {
            System.out.print(commandDesc() + "\n");
        }
    }

    private void handleLocalMode(CommandLine commandLine) {
        ExportRocksDBConfigToJsonRequestHeader.ConfigType type = Objects.requireNonNull(getConfigTypeList(commandLine)).get(0);
        String path = commandLine.getOptionValue("configPath").trim();
        if (StringUtils.isEmpty(path) || !new File(path).exists()) {
            System.out.print("Rocksdb path is invalid.\n");
            return;
        }
        path = Paths.get(path, type.toString()).toString();
        String exportFile = commandLine.hasOption("exportFile") ? commandLine.getOptionValue("exportFile").trim() : null;
        Map<String, JSONObject> configMap = getConfigMapFromRocksDB(path, type);
        if (configMap != null) {
            if (exportFile == null) {
                if (commandLine.hasOption("jsonEnable") && "false".equalsIgnoreCase(commandLine.getOptionValue("jsonEnable").trim())) {
                    printConfigMapJsonDisable(configMap);
                } else {
                    System.out.print(JSONObject.toJSONString(configMap, JSONWriter.Feature.PrettyFormat) + "\n");
                }
            } else {
                String jsonString = JSONObject.toJSONString(configMap, JSONWriter.Feature.PrettyFormat);
                try {
                    MixAll.string2File(jsonString, exportFile);
                } catch (IOException e) {
                    System.out.print("persist file " + exportFile + " exception" + e);
                }
            }
        }
    }

    private void checkRequiredArgsProvided(CommandLine commandLine, String mode,
        String... args) throws SubCommandException {
        for (String arg : args) {
            if (!commandLine.hasOption(arg)) {
                System.out.printf("%s Invalid args, please input %s\n", mode, String.join(",", args));
                throw new SubCommandException("Invalid args");
            }
        }
    }

    private List<ExportRocksDBConfigToJsonRequestHeader.ConfigType> getConfigTypeList(CommandLine commandLine) {
        List<ExportRocksDBConfigToJsonRequestHeader.ConfigType> typeList = new ArrayList<>();
        if (commandLine.hasOption("configType")) {
            String configType = commandLine.getOptionValue("configType").trim();
            try {
                typeList.addAll(ExportRocksDBConfigToJsonRequestHeader.ConfigType.fromString(configType));
            } catch (IllegalArgumentException e) {
                System.out.print("Invalid configType: " + configType + " please input topics/subscriptionGroups/consumerOffsets \n");
                return null;
            }
        } else {
            typeList.addAll(Arrays.asList(ExportRocksDBConfigToJsonRequestHeader.ConfigType.values()));
        }
        return typeList;
    }

    private static void printConfigMapJsonDisable(Map<String, JSONObject> configMap) {
        AtomicLong count = new AtomicLong(0);
        for (Map.Entry<String, JSONObject> entry : configMap.entrySet()) {
            String configKey = entry.getKey();
            System.out.printf("type: %s", configKey);
            JSONObject jsonObject = entry.getValue();
            jsonObject.forEach((k, v) -> System.out.printf("%d, Key: %s, Value: %s%n", count.incrementAndGet(), k, v));
        }
    }

    private static Map<String, JSONObject> getConfigMapFromRocksDB(String path,
        ExportRocksDBConfigToJsonRequestHeader.ConfigType configType) {

        if (ExportRocksDBConfigToJsonRequestHeader.ConfigType.CONSUMER_OFFSETS.equals(configType)) {
            return loadConsumerOffsets(path);
        }

        ConfigRocksDBStorage configRocksDBStorage = new ConfigRocksDBStorage(path, true);
        configRocksDBStorage.start();
        RocksIterator iterator = configRocksDBStorage.iterator();
        try {
            final Map<String, JSONObject> configMap = new HashMap<>();
            final JSONObject configTable = new JSONObject();
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
            if (kvDataVersion != null) {
                configMap.put("dataVersion",
                    JSONObject.parseObject(new String(kvDataVersion, DataConverter.CHARSET_UTF8)));
            }

            if (ExportRocksDBConfigToJsonRequestHeader.ConfigType.TOPICS.equals(configType)) {
                configMap.put("topicConfigTable", configTable);
            }
            if (ExportRocksDBConfigToJsonRequestHeader.ConfigType.SUBSCRIPTION_GROUPS.equals(configType)) {
                configMap.put("subscriptionGroupTable", configTable);
            }
            return configMap;
        } catch (Exception e) {
            System.out.print("Error occurred while converting RocksDB kv config to json, " + "configType=" + configType + ", " + e.getMessage() + "\n");
        } finally {
            configRocksDBStorage.shutdown();
        }
        return null;
    }

    private void handleRpcMode(CommandLine commandLine, RPCHook rpcHook,
        List<ExportRocksDBConfigToJsonRequestHeader.ConfigType> type) {
        String nameserverAddr = commandLine.hasOption('n') ? commandLine.getOptionValue("nameserverAddr").trim() : null;
        String inputBrokerAddr = commandLine.hasOption('b') ? commandLine.getOptionValue('b').trim() : null;
        String clusterName = commandLine.hasOption('c') ? commandLine.getOptionValue('c').trim() : null;

        DefaultMQAdminExt defaultMQAdminExt = new DefaultMQAdminExt(rpcHook, 30 * 1000);
        defaultMQAdminExt.setInstanceName(Long.toString(System.currentTimeMillis()));
        defaultMQAdminExt.setNamesrvAddr(nameserverAddr);

        List<CompletableFuture<Void>> futureList = new ArrayList<>();

        try {
            defaultMQAdminExt.start();
            if (clusterName != null) {
                ClusterInfo clusterInfo = defaultMQAdminExt.examineBrokerClusterInfo();
                Map<String, Set<String>> clusterAddrTable = clusterInfo.getClusterAddrTable();
                Map<String, BrokerData> brokerAddrTable = clusterInfo.getBrokerAddrTable();
                if (clusterAddrTable.get(clusterName) == null) {
                    System.out.print("clusterAddrTable is empty");
                    return;
                }
                for (Map.Entry<String, BrokerData> entry : brokerAddrTable.entrySet()) {
                    String brokerName = entry.getKey();
                    BrokerData brokerData = entry.getValue();
                    String brokerAddr = brokerData.getBrokerAddrs().get(0L);
                    futureList.add(sendRequest(type, defaultMQAdminExt, brokerAddr, brokerName));
                }
            } else if (inputBrokerAddr != null) {
                futureList.add(sendRequest(type, defaultMQAdminExt, inputBrokerAddr, null));
            }
            CompletableFuture.allOf(futureList.toArray(new CompletableFuture[0])).whenComplete(
                (v, t) -> System.out.print("broker export done.")
            ).join();
        } catch (Exception e) {
            throw new RuntimeException(this.getClass().getSimpleName() + " command failed", e);
        } finally {
            defaultMQAdminExt.shutdown();
        }
    }

    private CompletableFuture<Void> sendRequest(List<ExportRocksDBConfigToJsonRequestHeader.ConfigType> type,
        DefaultMQAdminExt defaultMQAdminExt, String brokerAddr, String brokerName) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                defaultMQAdminExt.exportRocksDBConfigToJson(brokerAddr, type);
            } catch (Throwable t) {
                System.out.print((brokerName != null) ? brokerName : brokerAddr + " export error");
                throw new CompletionException(this.getClass().getSimpleName() + " command failed", t);
            }
            return null;
        });
    }

    private static Map<String, JSONObject> loadConsumerOffsets(String path) {
        ConfigRocksDBStorage configRocksDBStorage = new ConfigRocksDBStorage(path, true);
        configRocksDBStorage.start();
        RocksIterator iterator = configRocksDBStorage.iterator();
        try {
            final Map<String, JSONObject> configMap = new HashMap<>();
            final JSONObject configTable = new JSONObject();
            iterator.seekToFirst();
            while (iterator.isValid()) {
                final byte[] key = iterator.key();
                final byte[] value = iterator.value();
                final String name = new String(key, DataConverter.CHARSET_UTF8);
                final String config = new String(value, DataConverter.CHARSET_UTF8);
                final RocksDBOffsetSerializeWrapper jsonObject = JSONObject.parseObject(config, RocksDBOffsetSerializeWrapper.class);
                configTable.put(name, jsonObject.getOffsetTable());
                iterator.next();
            }
            configMap.put("offsetTable", configTable);
            return configMap;
        } catch (Exception e) {
            System.out.print("Error occurred while converting RocksDB kv config to json, " + "configType=consumerOffsets, " + e.getMessage() + "\n");
        } finally {
            configRocksDBStorage.shutdown();
        }
        return null;
    }

    static class RocksDBOffsetSerializeWrapper {
        private ConcurrentMap<Integer, Long> offsetTable = new ConcurrentHashMap<>(16);

        public ConcurrentMap<Integer, Long> getOffsetTable() {
            return offsetTable;
        }

        public void setOffsetTable(ConcurrentMap<Integer, Long> offsetTable) {
            this.offsetTable = offsetTable;
        }
    }
}