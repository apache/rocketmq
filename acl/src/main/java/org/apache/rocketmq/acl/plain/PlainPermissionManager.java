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
package org.apache.rocketmq.acl.plain;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.acl.PermissionChecker;
import org.apache.rocketmq.acl.common.AclConstants;
import org.apache.rocketmq.acl.common.AclException;
import org.apache.rocketmq.acl.common.AclUtils;
import org.apache.rocketmq.acl.common.Permission;
import org.apache.rocketmq.common.AclConfig;
import org.apache.rocketmq.common.DataVersion;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.NamespaceAndPerm;
import org.apache.rocketmq.common.OperationType;
import org.apache.rocketmq.common.PlainAccessConfig;
import org.apache.rocketmq.common.ResourceAndPerm;
import org.apache.rocketmq.common.ResourceType;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.topic.TopicValidator;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.srvutil.AclFileWatchService;

import static org.apache.rocketmq.common.protocol.NamespaceUtil.NAMESPACE_SEPARATOR;

public class PlainPermissionManager {

    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.COMMON_LOGGER_NAME);

    private String fileHome = System.getProperty(MixAll.ROCKETMQ_HOME_PROPERTY,
        System.getenv(MixAll.ROCKETMQ_HOME_ENV));

    private String defaultAclDir;

    private String defaultAclFile;

    private Map<String/** fileFullPath **/, Map<String/** AccessKey **/, PlainAccessResource>> aclPlainAccessResourceMap = new HashMap<>();

    private Map<String/** AccessKey **/, String/** fileFullPath **/> accessKeyTable = new HashMap<>();

    private List<RemoteAddressStrategy> globalWhiteRemoteAddressStrategy = new ArrayList<>();

    private RemoteAddressStrategyFactory remoteAddressStrategyFactory = new RemoteAddressStrategyFactory();

    private Map<String/** fileFullPath **/, List<RemoteAddressStrategy>> globalWhiteRemoteAddressStrategyMap = new HashMap<>();

    private boolean isWatchStart;

    private Map<String/** fileFullPath **/, DataVersion> dataVersionMap = new HashMap<>();

    @Deprecated
    private final DataVersion dataVersion = new DataVersion();

    private List<String> fileList = new ArrayList<>();

    private final PermissionChecker permissionChecker = new PlainPermissionChecker();

    public PlainPermissionManager() {
        this.defaultAclDir = MixAll.dealFilePath(fileHome + File.separator + "conf" + File.separator + "acl");
        this.defaultAclFile = MixAll.dealFilePath(fileHome + File.separator + System.getProperty("rocketmq.acl.plain.file", "conf" + File.separator + "plain_acl.yml"));
        load();
        watch();
    }

    public List<String> getAllAclFiles(String path) {
        if (!new File(path).exists()) {
            log.info("The default acl dir {} is not exist", path);
            return new ArrayList<>();
        }
        List<String> allAclFileFullPath = new ArrayList<>();
        File file = new File(path);
        File[] files = file.listFiles();
        for (int i = 0; i < files.length; i++) {
            String fileName = files[i].getAbsolutePath();
            File f = new File(fileName);
            if (fileName.equals(fileHome + MixAll.ACL_CONF_TOOLS_FILE)) {
                continue;
            } else if (fileName.endsWith(".yml") || fileName.endsWith(".yaml")) {
                allAclFileFullPath.add(fileName);
            } else if (f.isDirectory()) {
                allAclFileFullPath.addAll(getAllAclFiles(fileName));
            }
        }
        return allAclFileFullPath;
    }

    public void load() {
        if (fileHome == null || fileHome.isEmpty()) {
            return;
        }

        Map<String, Map<String, PlainAccessResource>> aclPlainAccessResourceMap = new HashMap<>();
        Map<String, String> accessKeyTable = new HashMap<>();
        List<RemoteAddressStrategy> globalWhiteRemoteAddressStrategy = new ArrayList<>();
        Map<String, List<RemoteAddressStrategy>> globalWhiteRemoteAddressStrategyMap = new HashMap<>();
        Map<String, DataVersion> dataVersionMap = new HashMap<>();

        assureAclConfigFilesExist();

        fileList = getAllAclFiles(defaultAclDir);
        if (new File(defaultAclFile).exists() && !fileList.contains(defaultAclFile)) {
            fileList.add(defaultAclFile);
        }

        for (int i = 0; i < fileList.size(); i++) {
            final String currentFile = MixAll.dealFilePath(fileList.get(i));
            JSONObject plainAclConfData = AclUtils.getYamlDataObject(currentFile,
                JSONObject.class);
            if (plainAclConfData == null || plainAclConfData.isEmpty()) {
                log.warn("No data in file {}", currentFile);
                continue;
            }
            log.info("Broker plain acl conf data is : {}", plainAclConfData.toString());

            List<RemoteAddressStrategy> globalWhiteRemoteAddressStrategyList = new ArrayList<>();
            JSONArray globalWhiteRemoteAddressesList = plainAclConfData.getJSONArray(AclConstants.CONFIG_GLOBAL_WHITE_ADDRS);
            if (globalWhiteRemoteAddressesList != null && !globalWhiteRemoteAddressesList.isEmpty()) {
                for (int j = 0; j < globalWhiteRemoteAddressesList.size(); j++) {
                    globalWhiteRemoteAddressStrategyList.add(remoteAddressStrategyFactory.
                        getRemoteAddressStrategy(globalWhiteRemoteAddressesList.getString(j)));
                }
            }
            if (globalWhiteRemoteAddressStrategyList.size() > 0) {
                globalWhiteRemoteAddressStrategyMap.put(currentFile, globalWhiteRemoteAddressStrategyList);
                globalWhiteRemoteAddressStrategy.addAll(globalWhiteRemoteAddressStrategyList);
            }

            JSONArray accounts = plainAclConfData.getJSONArray(AclConstants.CONFIG_ACCOUNTS);
            Map<String, PlainAccessResource> plainAccessResourceMap = new HashMap<>();
            if (accounts != null && !accounts.isEmpty()) {
                List<PlainAccessConfig> plainAccessConfigList = accounts.toJavaList(PlainAccessConfig.class);
                for (PlainAccessConfig plainAccessConfig : plainAccessConfigList) {
                    PlainAccessResource plainAccessResource = buildPlainAccessResource(plainAccessConfig);
                    //AccessKey can not be defined in multiple ACL files
                    if (accessKeyTable.get(plainAccessResource.getAccessKey()) == null) {
                        plainAccessResourceMap.put(plainAccessResource.getAccessKey(), plainAccessResource);
                        accessKeyTable.put(plainAccessResource.getAccessKey(), currentFile);
                    } else {
                        log.warn("The accessKey {} is repeated in multiple ACL files", plainAccessResource.getAccessKey());
                    }
                }
            }
            if (plainAccessResourceMap.size() > 0) {
                aclPlainAccessResourceMap.put(currentFile, plainAccessResourceMap);
            }

            JSONArray tempDataVersion = plainAclConfData.getJSONArray(AclConstants.CONFIG_DATA_VERSION);
            DataVersion dataVersion = new DataVersion();
            if (tempDataVersion != null && !tempDataVersion.isEmpty()) {
                List<DataVersion> dataVersions = tempDataVersion.toJavaList(DataVersion.class);
                DataVersion firstElement = dataVersions.get(0);
                dataVersion.assignNewOne(firstElement);
            }
            dataVersionMap.put(currentFile, dataVersion);
        }

        if (dataVersionMap.containsKey(defaultAclFile)) {
            this.dataVersion.assignNewOne(dataVersionMap.get(defaultAclFile));
        }
        this.dataVersionMap = dataVersionMap;
        this.globalWhiteRemoteAddressStrategyMap = globalWhiteRemoteAddressStrategyMap;
        this.globalWhiteRemoteAddressStrategy = globalWhiteRemoteAddressStrategy;
        this.aclPlainAccessResourceMap = aclPlainAccessResourceMap;
        this.accessKeyTable = accessKeyTable;
    }

    /**
     * Currently GlobalWhiteAddress is defined in {@link #defaultAclFile}, so make sure it exists.
     */
    private void assureAclConfigFilesExist() {
        final Path defaultAclFilePath = Paths.get(this.defaultAclFile);
        if (!Files.exists(defaultAclFilePath)) {
            try {
                Files.createFile(defaultAclFilePath);
            } catch (FileAlreadyExistsException e) {
                // Maybe created by other threads
            } catch (IOException e) {
                log.error("Error in creating " + this.defaultAclFile, e);
                throw new AclException(e.getMessage());
            }
        }
    }

    public void load(String aclFilePath) {
        aclFilePath = MixAll.dealFilePath(aclFilePath);
        Map<String, PlainAccessResource> plainAccessResourceMap = new HashMap<>();
        List<RemoteAddressStrategy> globalWhiteRemoteAddressStrategy = new ArrayList<>();

        JSONObject plainAclConfData = AclUtils.getYamlDataObject(aclFilePath,
            JSONObject.class);
        if (plainAclConfData == null || plainAclConfData.isEmpty()) {
            log.warn("No data in {}, skip it", aclFilePath);
            return;
        }
        log.info("Broker plain acl conf data is : {}", plainAclConfData.toString());
        JSONArray globalWhiteRemoteAddressesList = plainAclConfData.getJSONArray(AclConstants.CONFIG_GLOBAL_WHITE_ADDRS);
        if (globalWhiteRemoteAddressesList != null && !globalWhiteRemoteAddressesList.isEmpty()) {
            for (int i = 0; i < globalWhiteRemoteAddressesList.size(); i++) {
                globalWhiteRemoteAddressStrategy.add(remoteAddressStrategyFactory.
                    getRemoteAddressStrategy(globalWhiteRemoteAddressesList.getString(i)));
            }
        }

        this.globalWhiteRemoteAddressStrategy.addAll(globalWhiteRemoteAddressStrategy);
        if (this.globalWhiteRemoteAddressStrategyMap.get(aclFilePath) != null) {
            List<RemoteAddressStrategy> remoteAddressStrategyList = this.globalWhiteRemoteAddressStrategyMap.get(aclFilePath);
            for (int i = 0; i < remoteAddressStrategyList.size(); i++) {
                this.globalWhiteRemoteAddressStrategy.remove(remoteAddressStrategyList.get(i));
            }
            this.globalWhiteRemoteAddressStrategyMap.put(aclFilePath, globalWhiteRemoteAddressStrategy);
        }

        JSONArray accounts = plainAclConfData.getJSONArray(AclConstants.CONFIG_ACCOUNTS);
        if (accounts != null && !accounts.isEmpty()) {
            List<PlainAccessConfig> plainAccessConfigList = accounts.toJavaList(PlainAccessConfig.class);
            for (PlainAccessConfig plainAccessConfig : plainAccessConfigList) {
                PlainAccessResource plainAccessResource = buildPlainAccessResource(plainAccessConfig);
                //AccessKey can not be defined in multiple ACL files
                String oldPath = this.accessKeyTable.get(plainAccessResource.getAccessKey());
                if (oldPath == null || aclFilePath.equals(oldPath)) {
                    plainAccessResourceMap.put(plainAccessResource.getAccessKey(), plainAccessResource);
                    this.accessKeyTable.put(plainAccessResource.getAccessKey(), aclFilePath);
                }
            }
        }

        // For loading dataversion part just
        JSONArray tempDataVersion = plainAclConfData.getJSONArray(AclConstants.CONFIG_DATA_VERSION);
        DataVersion dataVersion = new DataVersion();
        if (tempDataVersion != null && !tempDataVersion.isEmpty()) {
            List<DataVersion> dataVersions = tempDataVersion.toJavaList(DataVersion.class);
            DataVersion firstElement = dataVersions.get(0);
            dataVersion.assignNewOne(firstElement);
        }

        this.aclPlainAccessResourceMap.put(aclFilePath, plainAccessResourceMap);
        this.dataVersionMap.put(aclFilePath, dataVersion);
        if (aclFilePath.equals(defaultAclFile)) {
            this.dataVersion.assignNewOne(dataVersion);
        }
    }

    @Deprecated
    public String getAclConfigDataVersion() {
        return this.dataVersion.toJson();
    }

    public Map<String, DataVersion> getDataVersionMap() {
        return this.dataVersionMap;
    }

    public Map<String, Object> updateAclConfigFileVersion(String aclFileName, Map<String, Object> updateAclConfigMap) {

        Object dataVersions = updateAclConfigMap.get(AclConstants.CONFIG_DATA_VERSION);
        DataVersion dataVersion = new DataVersion();
        if (dataVersions != null) {
            List<Map<String, Object>> dataVersionList = (List<Map<String, Object>>) dataVersions;
            if (dataVersionList.size() > 0) {
                dataVersion.setTimestamp((long) dataVersionList.get(0).get("timestamp"));
                dataVersion.setCounter(new AtomicLong(Long.parseLong(dataVersionList.get(0).get("counter").toString())));
            }
        }
        dataVersion.nextVersion();
        List<Map<String, Object>> versionElement = new ArrayList<>();
        Map<String, Object> accountsMap = new LinkedHashMap<>();
        accountsMap.put(AclConstants.CONFIG_COUNTER, dataVersion.getCounter().longValue());
        accountsMap.put(AclConstants.CONFIG_TIME_STAMP, dataVersion.getTimestamp());

        versionElement.add(accountsMap);
        updateAclConfigMap.put(AclConstants.CONFIG_DATA_VERSION, versionElement);

        dataVersionMap.put(aclFileName, dataVersion);

        return updateAclConfigMap;
    }

    public boolean updateAccessConfig(PlainAccessConfig plainAccessConfig) {

        if (plainAccessConfig == null) {
            log.error("Parameter value plainAccessConfig is null,Please check your parameter");
            throw new AclException("Parameter value plainAccessConfig is null, Please check your parameter");
        }
        checkPlainAccessConfig(plainAccessConfig);

        Permission.checkResourcePerms(plainAccessConfig.getTopicPerms());
        Permission.checkResourcePerms(plainAccessConfig.getGroupPerms());

        if (accessKeyTable.containsKey(plainAccessConfig.getAccessKey())) {
            Map<String, Object> updateAccountMap = null;
            String aclFileName = accessKeyTable.get(plainAccessConfig.getAccessKey());
            Map<String, Object> aclAccessConfigMap = AclUtils.getYamlDataObject(aclFileName, Map.class);
            List<Map<String, Object>> accounts = (List<Map<String, Object>>) aclAccessConfigMap.get(AclConstants.CONFIG_ACCOUNTS);
            if (null != accounts) {
                for (Map<String, Object> account : accounts) {
                    if (account.get(AclConstants.CONFIG_ACCESS_KEY).equals(plainAccessConfig.getAccessKey())) {
                        // Update acl access config elements
                        accounts.remove(account);
                        updateAccountMap = createAclAccessConfigMap(account, plainAccessConfig);
                        accounts.add(updateAccountMap);
                        aclAccessConfigMap.put(AclConstants.CONFIG_ACCOUNTS, accounts);
                        break;
                    }
                }
            } else {
                // Maybe deleted in file, add it back
                accounts = new LinkedList<>();
                updateAccountMap = createAclAccessConfigMap(null, plainAccessConfig);
                accounts.add(updateAccountMap);
                aclAccessConfigMap.put(AclConstants.CONFIG_ACCOUNTS, accounts);
            }
            Map<String, PlainAccessResource> accountMap = aclPlainAccessResourceMap.get(aclFileName);
            if (accountMap == null) {
                accountMap = new HashMap<>(1);
                accountMap.put(plainAccessConfig.getAccessKey(), buildPlainAccessResource(plainAccessConfig));
            } else if (accountMap.size() == 0) {
                accountMap.put(plainAccessConfig.getAccessKey(), buildPlainAccessResource(plainAccessConfig));
            } else {
                for (Map.Entry<String, PlainAccessResource> entry : accountMap.entrySet()) {
                    if (entry.getValue().getAccessKey().equals(plainAccessConfig.getAccessKey())) {
                        PlainAccessResource plainAccessResource = buildPlainAccessResource(plainAccessConfig);
                        accountMap.put(entry.getKey(), plainAccessResource);
                        break;
                    }
                }
            }
            aclPlainAccessResourceMap.put(aclFileName, accountMap);
            return AclUtils.writeDataObject(aclFileName, updateAclConfigFileVersion(aclFileName, aclAccessConfigMap));
        } else {
            String fileName = MixAll.dealFilePath(defaultAclFile);
            //Create acl access config elements on the default acl file
            if (aclPlainAccessResourceMap.get(defaultAclFile) == null || aclPlainAccessResourceMap.get(defaultAclFile).size() == 0) {
                try {
                    File defaultAclFile = new File(fileName);
                    if (!defaultAclFile.exists()) {
                        defaultAclFile.createNewFile();
                    }
                } catch (IOException e) {
                    log.warn("create default acl file has exception when update accessConfig. ", e);
                }
            }
            Map<String, Object> aclAccessConfigMap = AclUtils.getYamlDataObject(defaultAclFile, Map.class);
            if (aclAccessConfigMap == null) {
                aclAccessConfigMap = new HashMap<>();
                aclAccessConfigMap.put(AclConstants.CONFIG_ACCOUNTS, new ArrayList<>());
            }
            List<Map<String, Object>> accounts = (List<Map<String, Object>>) aclAccessConfigMap.get(AclConstants.CONFIG_ACCOUNTS);
            // When no accounts defined
            if (null == accounts) {
                accounts = new ArrayList<>();
            }
            accounts.add(createAclAccessConfigMap(null, plainAccessConfig));
            aclAccessConfigMap.put(AclConstants.CONFIG_ACCOUNTS, accounts);
            accessKeyTable.put(plainAccessConfig.getAccessKey(), fileName);
            if (aclPlainAccessResourceMap.get(fileName) == null) {
                Map<String, PlainAccessResource> plainAccessResourceMap = new HashMap<>(1);
                plainAccessResourceMap.put(plainAccessConfig.getAccessKey(), buildPlainAccessResource(plainAccessConfig));
                aclPlainAccessResourceMap.put(fileName, plainAccessResourceMap);
            } else {
                Map<String, PlainAccessResource> plainAccessResourceMap = aclPlainAccessResourceMap.get(fileName);
                plainAccessResourceMap.put(plainAccessConfig.getAccessKey(), buildPlainAccessResource(plainAccessConfig));
                aclPlainAccessResourceMap.put(fileName, plainAccessResourceMap);
            }
            return AclUtils.writeDataObject(defaultAclFile, updateAclConfigFileVersion(defaultAclFile, aclAccessConfigMap));
        }
    }

    public boolean updateAclAccount(PlainAccessConfig plainAccessConfig) {
        if (plainAccessConfig == null) {
            log.error("Parameter value plainAccessConfig is null,Please check your parameter");
            throw new AclException("Parameter value plainAccessConfig is null, Please check your parameter");
        }
        checkPlainAccessConfig(plainAccessConfig);

        //Permission.checkResourcePerms(plainAccessConfig.getTopicPerms());
        //Permission.checkResourcePerms(plainAccessConfig.getGroupPerms());
        if (accessKeyTable.containsKey(plainAccessConfig.getAccessKey())) {
            Map<String, Object> updateAccountMap = null;
            String aclFileName = accessKeyTable.get(plainAccessConfig.getAccessKey());
            Map<String, Object> aclAccessConfigMap = AclUtils.getYamlDataObject(aclFileName, Map.class);
            List<Map<String, Object>> accounts = (List<Map<String, Object>>) aclAccessConfigMap.get(AclConstants.CONFIG_ACCOUNTS);
            if (null != accounts) {
                for (Map<String, Object> account : accounts) {
                    if (account.get(AclConstants.CONFIG_ACCESS_KEY).equals(plainAccessConfig.getAccessKey())) {
                        // Update acl access config elements
                        accounts.remove(account);
                        updateAccountMap = createAclAccessConfigMap(account, plainAccessConfig);
                        accounts.add(updateAccountMap);
                        aclAccessConfigMap.put(AclConstants.CONFIG_ACCOUNTS, accounts);
                        break;
                    }
                }
            } else {
                // Maybe deleted in file, add it back
                accounts = new LinkedList<>();
                updateAccountMap = createAclAccessConfigMap(null, plainAccessConfig);
                accounts.add(updateAccountMap);
                aclAccessConfigMap.put(AclConstants.CONFIG_ACCOUNTS, accounts);
            }
            Map<String, PlainAccessResource> accountMap = aclPlainAccessResourceMap.get(aclFileName);
            PlainAccessResource updatePlainAccessResource = updatePlainAccessResource(updateAccountMap);
            if (accountMap == null) {
                accountMap = new HashMap<String, PlainAccessResource>(1);
                accountMap.put(plainAccessConfig.getAccessKey(), updatePlainAccessResource);
            } else if (accountMap.size() == 0) {
                accountMap.put(plainAccessConfig.getAccessKey(), updatePlainAccessResource);
            } else {
                for (Map.Entry<String, PlainAccessResource> entry : accountMap.entrySet()) {
                    if (entry.getValue().getAccessKey().equals(plainAccessConfig.getAccessKey())) {
                        accountMap.put(entry.getKey(), updatePlainAccessResource);
                        break;
                    }
                }
            }
            aclPlainAccessResourceMap.put(aclFileName, accountMap);
            return AclUtils.writeDataObject(aclFileName, updateAclConfigFileVersion(aclFileName, aclAccessConfigMap));
        } else {
            String fileName = MixAll.dealFilePath(defaultAclFile);
            //Create acl access config elements on the default acl file
            if (aclPlainAccessResourceMap.get(defaultAclFile) == null || aclPlainAccessResourceMap.get(defaultAclFile).size() == 0) {
                try {
                    File defaultAclFile = new File(fileName);
                    if (!defaultAclFile.exists()) {
                        defaultAclFile.createNewFile();
                    }
                } catch (IOException e) {
                    log.warn("create default acl file has exception when update accessConfig. ", e);
                }
            }
            Map<String, Object> aclAccessConfigMap = AclUtils.getYamlDataObject(defaultAclFile, Map.class);
            if (aclAccessConfigMap == null) {
                aclAccessConfigMap = new HashMap<>();
                aclAccessConfigMap.put(AclConstants.CONFIG_ACCOUNTS, new ArrayList<>());
            }
            List<Map<String, Object>> accounts = (List<Map<String, Object>>) aclAccessConfigMap.get(AclConstants.CONFIG_ACCOUNTS);
            // When no accounts defined
            if (null == accounts) {
                accounts = new ArrayList<>();
            }
            accounts.add(createAclAccessConfigMap(null, plainAccessConfig));
            aclAccessConfigMap.put(AclConstants.CONFIG_ACCOUNTS, accounts);
            accessKeyTable.put(plainAccessConfig.getAccessKey(), fileName);
            if (aclPlainAccessResourceMap.get(fileName) == null) {
                Map<String, PlainAccessResource> plainAccessResourceMap = new HashMap<>(1);
                plainAccessResourceMap.put(plainAccessConfig.getAccessKey(), buildPlainAccessResource(plainAccessConfig));
                aclPlainAccessResourceMap.put(fileName, plainAccessResourceMap);
            } else {
                Map<String, PlainAccessResource> plainAccessResourceMap = aclPlainAccessResourceMap.get(fileName);
                plainAccessResourceMap.put(plainAccessConfig.getAccessKey(), buildPlainAccessResource(plainAccessConfig));
                aclPlainAccessResourceMap.put(fileName, plainAccessResourceMap);
            }
            return AclUtils.writeDataObject(defaultAclFile, updateAclConfigFileVersion(defaultAclFile, aclAccessConfigMap));
        }
    }

    public PlainAccessResource updatePlainAccessResource(Map<String, Object> updateAccountMap) {
        PlainAccessResource updatePlainAccessResource = new PlainAccessResource();
        if (updateAccountMap.containsKey(AclConstants.CONFIG_ACCESS_KEY)) {
            updatePlainAccessResource.setAccessKey(updateAccountMap.get(AclConstants.CONFIG_ACCESS_KEY).toString());
        }
        if (updateAccountMap.containsKey(AclConstants.CONFIG_SECRET_KEY)) {
            updatePlainAccessResource.setSecretKey(updateAccountMap.get(AclConstants.CONFIG_SECRET_KEY).toString());
        }
        if (updateAccountMap.containsKey(AclConstants.CONFIG_WHITE_ADDR)) {
            updatePlainAccessResource.setRemoteAddressStrategy(remoteAddressStrategyFactory.
                getRemoteAddressStrategy(updateAccountMap.get(AclConstants.CONFIG_WHITE_ADDR).toString()));
        }
        if (updateAccountMap.containsKey(AclConstants.CONFIG_ADMIN_ROLE)) {
            updatePlainAccessResource.setAdmin(Boolean.parseBoolean(updateAccountMap.get(AclConstants.CONFIG_ADMIN_ROLE).toString()));
        }
        if (updateAccountMap.containsKey(AclConstants.CONFIG_DEFAULT_TOPIC_PERM)) {
            updatePlainAccessResource.setDefaultTopicPerm(Permission.
                parsePermFromString(updateAccountMap.get(AclConstants.CONFIG_DEFAULT_TOPIC_PERM).toString()));
        }
        if (updateAccountMap.containsKey(AclConstants.CONFIG_DEFAULT_GROUP_PERM)) {
            updatePlainAccessResource.setDefaultGroupPerm(Permission.
                parsePermFromString(updateAccountMap.get(AclConstants.CONFIG_DEFAULT_GROUP_PERM).toString()));
        }
        if (updateAccountMap.containsKey(AclConstants.CONFIG_RESOURCE_PERMS)) {
            List<LinkedHashMap<String, Object>> resourceAndPermList = (List<LinkedHashMap<String, Object>>)updateAccountMap.get(AclConstants.CONFIG_RESOURCE_PERMS);
            for (LinkedHashMap resourceAndPerm : resourceAndPermList) {
                String resource = resourceAndPerm.get("resource").toString();
                String namespace = resourceAndPerm.get("namespace").toString();
                String type = resourceAndPerm.get("type").toString();
                String perm = resourceAndPerm.get("perm").toString();
                if (namespace == null || namespace.isEmpty()) {
                    if (type.equals(ResourceType.GROUP)) {
                        updatePlainAccessResource.addResourceAndPerm(MixAll.getRetryTopic(resource), Permission.parsePermFromString(perm));
                    } else {
                        updatePlainAccessResource.addResourceAndPerm(resource, Permission.parsePermFromString(perm));
                    }
                } else {
                    if (type.equals(ResourceType.GROUP)) {
                        updatePlainAccessResource.addResourceAndPerm(
                            MixAll.RETRY_GROUP_TOPIC_PREFIX + namespace + NAMESPACE_SEPARATOR + resource, Permission.parsePermFromString(perm));
                    } else {
                        updatePlainAccessResource.addResourceAndPerm(namespace + resource, Permission.parsePermFromString(perm));
                    }
                }

            }
        }
        if (updateAccountMap.containsKey(AclConstants.CONFIG_NAMESPACE_PERMS)) {
            List<LinkedHashMap<String, String>> namespaceAndPermList = (List<LinkedHashMap<String, String>>)updateAccountMap.get(AclConstants.CONFIG_NAMESPACE_PERMS);
            Map<String, Map<String, Byte>> namespacePermMap = new HashMap<>();
            for (LinkedHashMap<String, String> namespaceAndPerm : namespaceAndPermList) {
                String namespace = namespaceAndPerm.get("namespace");
                String topicPerm = namespaceAndPerm.get("topicPerm");
                String groupPerm = namespaceAndPerm.get("groupPerm");
                Map<String, Byte> permMap = new HashMap<>(2);
                if (topicPerm != null && !topicPerm.isEmpty()) {
                    permMap.put(AclConstants.CONFIG_TOPIC_PERM, Permission.parsePermFromString(topicPerm));
                }
                if (groupPerm != null && !groupPerm.isEmpty()) {
                    permMap.put(AclConstants.CONFIG_GROUP_PERM, Permission.parsePermFromString(groupPerm));
                }
                namespacePermMap.put(namespace, permMap);
            }
            updatePlainAccessResource.setNamespacePermMap(namespacePermMap);
        }
        return updatePlainAccessResource;
    }

    public boolean updateAclResourcePerms(String accesskey, ResourceAndPerm resourceAndPerm, OperationType operationType) {
        if (accesskey == null || resourceAndPerm == null || operationType == null) {
            log.error("Parameter: accesskey or resourceAndPerm or operationType is null, Please check your parameter");
            throw new AclException("The parameter of updateAclResourcePerms command: accesskey or resourceAndPerm or operationType is null, Please check your parameter");
        }
        if (operationType == OperationType.ADD || operationType == OperationType.UPDATE) {
            checkResourceAndPerm(resourceAndPerm);
        }

        if (accessKeyTable.containsKey(accesskey)) {
            Map<String, Object> updateAccountMap = null;
            String aclFileName = accessKeyTable.get(accesskey);
            Map<String, Object> aclAccessConfigMap = AclUtils.getYamlDataObject(aclFileName, Map.class);
            List<Map<String, Object>> accounts = (List<Map<String, Object>>) aclAccessConfigMap.get(AclConstants.CONFIG_ACCOUNTS);
            if (null != accounts) {
                for (Map<String, Object> account : accounts) {
                    if (account.get(AclConstants.CONFIG_ACCESS_KEY).equals(accesskey)) {
                        // Update acl access config elements
                        accounts.remove(account);
                        updateAccountMap = updateAclAccessConfigMap(account, resourceAndPerm, operationType);
                        accounts.add(updateAccountMap);
                        aclAccessConfigMap.put(AclConstants.CONFIG_ACCOUNTS, accounts);
                        break;
                    }
                }
            } else {
                // Maybe deleted in file, add it back
                accounts = new LinkedList<>();
                updateAccountMap = updateAclAccessConfigMap(null, resourceAndPerm, operationType);
                updateAccountMap.put(AclConstants.CONFIG_ACCESS_KEY, accesskey);
                accounts.add(updateAccountMap);
                aclAccessConfigMap.put(AclConstants.CONFIG_ACCOUNTS, accounts);
            }
            Map<String, PlainAccessResource> accountMap = aclPlainAccessResourceMap.get(aclFileName);
            PlainAccessResource updatePlainAccessResource = updatePlainAccessResource(updateAccountMap);
            if (accountMap == null) {
                accountMap = new HashMap<String, PlainAccessResource>(1);
                accountMap.put(accesskey, updatePlainAccessResource);
            } else if (accountMap.size() == 0) {
                accountMap.put(accesskey, updatePlainAccessResource);
            } else {
                for (Map.Entry<String, PlainAccessResource> entry : accountMap.entrySet()) {
                    if (entry.getValue().getAccessKey().equals(accesskey)) {
                        accountMap.put(entry.getKey(), updatePlainAccessResource);
                        break;
                    }
                }
            }
            aclPlainAccessResourceMap.put(aclFileName, accountMap);
            return AclUtils.writeDataObject(aclFileName, updateAclConfigFileVersion(aclFileName, aclAccessConfigMap));
        } else {
            throw new AclException("The accesskey for update acl resource perms is not exist in the acl config.");
        }

    }

    public Map<String, Object> updateAclAccessConfigMap(Map<String, Object> existedAccountMap,
        ResourceAndPerm resourceAndPerm, OperationType operationType) {
        Map<String, Object> newAccountsMap = null;
        if (existedAccountMap == null) {
            newAccountsMap = new LinkedHashMap<String, Object>();
        } else {
            newAccountsMap = existedAccountMap;
        }

        List<LinkedHashMap> resourceAndPerms = (List<LinkedHashMap>)newAccountsMap.get(AclConstants.CONFIG_RESOURCE_PERMS);
        if (resourceAndPerms == null) {
            resourceAndPerms = new ArrayList<>();
        }

        if (operationType == OperationType.ADD) {
            LinkedHashMap<String, Object> addResourceAndPerm = new LinkedHashMap<String, Object>(4);
            addResourceAndPerm.put("resource", resourceAndPerm.getResource());
            addResourceAndPerm.put("type", resourceAndPerm.getType());
            addResourceAndPerm.put("namespace", resourceAndPerm.getNamespace());
            addResourceAndPerm.put("perm", resourceAndPerm.getPerm());
            resourceAndPerms.add(addResourceAndPerm);
        } else {
            Iterator iterator = resourceAndPerms.iterator();
            while (iterator.hasNext()) {
                LinkedHashMap<String, Object> curResourceAndPerm = (LinkedHashMap)iterator.next();
                if (curResourceAndPerm.get("resource").toString().equals(resourceAndPerm.getResource()) && curResourceAndPerm.get("type").toString().equals(resourceAndPerm.getType().toString())) {
                    if ((curResourceAndPerm.get("namespace") != null && resourceAndPerm.getNamespace() != null
                        && !resourceAndPerm.getNamespace().isEmpty() && curResourceAndPerm.get("namespace").toString().equals(resourceAndPerm.getNamespace())) ||
                        (curResourceAndPerm.get("namespace") == null && resourceAndPerm.getNamespace() == null)) {
                        iterator.remove();
                        break;
                    }
                }
            }
            if (operationType == OperationType.UPDATE) {
                LinkedHashMap addResourceAndPerm = new LinkedHashMap<String, Object>(4);
                addResourceAndPerm.put("resource", resourceAndPerm.getResource());
                addResourceAndPerm.put("type", resourceAndPerm.getType());
                addResourceAndPerm.put("namespace", resourceAndPerm.getNamespace());
                addResourceAndPerm.put("perm", resourceAndPerm.getPerm());
                resourceAndPerms.add(addResourceAndPerm);
            }
        }

        newAccountsMap.put(AclConstants.CONFIG_RESOURCE_PERMS, resourceAndPerms);

        return newAccountsMap;
    }

    public boolean updateAclNamespacePerms(String accesskey, List<NamespaceAndPerm> namespaceAndPerms, OperationType operationType) {
        if (accesskey == null || operationType == null || namespaceAndPerms == null) {
            log.error("Parameter: accesskey or operationType is null, Please check your parameter");
            throw new AclException("The parameter of updateAclResourcePerms command: accesskey or operationType is null, Please check your parameter");
        }

        if (accessKeyTable.containsKey(accesskey)) {
            Map<String, Object> updateAccountMap = null;
            String aclFileName = accessKeyTable.get(accesskey);
            Map<String, Object> aclAccessConfigMap = AclUtils.getYamlDataObject(aclFileName, Map.class);
            List<Map<String, Object>> accounts = (List<Map<String, Object>>) aclAccessConfigMap.get(AclConstants.CONFIG_ACCOUNTS);
            if (null != accounts) {
                for (Map<String, Object> account : accounts) {
                    if (account.get(AclConstants.CONFIG_ACCESS_KEY).equals(accesskey)) {
                        // Update acl access config elements
                        accounts.remove(account);
                        updateAccountMap = updateAclAccessConfigMap(account, namespaceAndPerms, operationType);
                        accounts.add(updateAccountMap);
                        aclAccessConfigMap.put(AclConstants.CONFIG_ACCOUNTS, accounts);
                        break;
                    }
                }
            } else {
                // Maybe deleted in file, add it back
                accounts = new LinkedList<>();
                updateAccountMap = updateAclAccessConfigMap(null, namespaceAndPerms, operationType);
                updateAccountMap.put(AclConstants.CONFIG_ACCESS_KEY, accesskey);
                accounts.add(updateAccountMap);
                aclAccessConfigMap.put(AclConstants.CONFIG_ACCOUNTS, accounts);
            }
            Map<String, PlainAccessResource> accountMap = aclPlainAccessResourceMap.get(aclFileName);
            PlainAccessResource updatePlainAccessResource = updatePlainAccessResource(updateAccountMap);
            if (accountMap == null) {
                accountMap = new HashMap<String, PlainAccessResource>(1);
                accountMap.put(accesskey, updatePlainAccessResource);
            } else if (accountMap.size() == 0) {
                accountMap.put(accesskey, updatePlainAccessResource);
            } else {
                for (Map.Entry<String, PlainAccessResource> entry : accountMap.entrySet()) {
                    if (entry.getValue().getAccessKey().equals(accesskey)) {
                        accountMap.put(entry.getKey(), updatePlainAccessResource);
                        break;
                    }
                }
            }
            aclPlainAccessResourceMap.put(aclFileName, accountMap);
            return AclUtils.writeDataObject(aclFileName, updateAclConfigFileVersion(aclFileName, aclAccessConfigMap));
        } else {
            throw new AclException("The accesskey for update acl namespace perms is not exist in the acl config.");
        }


    }

    public Map<String, Object> updateAclAccessConfigMap(Map<String, Object> existedAccountMap,
        List<NamespaceAndPerm> namespaceAndPerms, OperationType operationType) {
        Map<String, Object> newAccountsMap = null;
        if (existedAccountMap == null) {
            newAccountsMap = new LinkedHashMap<String, Object>();
        } else {
            newAccountsMap = existedAccountMap;
        }

        List<LinkedHashMap<String, String>> namespaceAndPermList = (List<LinkedHashMap<String, String>>)newAccountsMap.get(AclConstants.CONFIG_NAMESPACE_PERMS);
        if (namespaceAndPermList == null) {
            namespaceAndPermList = new ArrayList<>();
        }

        if (operationType == OperationType.ADD) {
            for (NamespaceAndPerm namespaceAndPerm : namespaceAndPerms) {
                LinkedHashMap<String, String> linkedHashMap = new LinkedHashMap<>(3);
                linkedHashMap.put("namespace", namespaceAndPerm.getNamespace());
                linkedHashMap.put("topicPerm", namespaceAndPerm.getTopicPerm());
                linkedHashMap.put("groupPerm", namespaceAndPerm.getGroupPerm());
                namespaceAndPermList.add(linkedHashMap);
            }
            //namespaceAndPermList.addAll(namespaceAndPerms);
            /*
            for (NamespaceAndPerm namespaceAndPerm : namespaceAndPerms) {
                namespaceAndPermList.add(namespaceAndPerm);
            }
             */
        } else {
            Iterator iterator = namespaceAndPermList.iterator();
            while (iterator.hasNext()) {
                LinkedHashMap<String, String> curNamespaceAndPerm = (LinkedHashMap<String, String>)iterator.next();
                for (NamespaceAndPerm namespaceAndPerm : namespaceAndPerms) {
                    if (curNamespaceAndPerm.get("namespace").equals(namespaceAndPerm.getNamespace())) {
                        iterator.remove();
                    }
                }
            }
            if (operationType == OperationType.UPDATE) {
                for (NamespaceAndPerm namespaceAndPerm : namespaceAndPerms) {
                    LinkedHashMap<String, String> linkedHashMap = new LinkedHashMap<>(3);
                    linkedHashMap.put("namespace", namespaceAndPerm.getNamespace());
                    linkedHashMap.put("topicPerm", namespaceAndPerm.getTopicPerm());
                    linkedHashMap.put("groupPerm", namespaceAndPerm.getGroupPerm());
                    namespaceAndPermList.add(linkedHashMap);
                }
            }
        }

        newAccountsMap.put(AclConstants.CONFIG_NAMESPACE_PERMS, namespaceAndPermList);

        return newAccountsMap;
    }

    public PlainAccessConfig getAccesskeyConfg(String accesskey) {
        if (accesskey == null || accesskey.isEmpty()) {
            log.error("Parameter value accesskey is null,Please check your parameter");
            throw new AclException("Parameter value accesskey is null, Please check your parameter");
        }

        if (accessKeyTable.containsKey(accesskey)) {
            String aclFileName = accessKeyTable.get(accesskey);
            JSONObject plainAclConfData = AclUtils.getYamlDataObject(aclFileName, JSONObject.class);
            JSONArray accounts = plainAclConfData.getJSONArray(AclConstants.CONFIG_ACCOUNTS);
            if (accounts != null && !accounts.isEmpty()) {
                List<PlainAccessConfig> plainAccessConfigs = accounts.toJavaList(PlainAccessConfig.class);
                for (PlainAccessConfig plainAccessConfig : plainAccessConfigs) {
                    if (plainAccessConfig.getAccessKey().equals(accesskey)) {
                        return plainAccessConfig;
                    } else {
                        continue;
                    }
                }
            }
        } else {
            throw new AclException("The accesskey:" + accesskey + " is not configured in acl config file.");
        }
        return null;
    }

    public Map<String, Object> createAclAccessConfigMap(Map<String, Object> existedAccountMap,
        PlainAccessConfig plainAccessConfig) {

        Map<String, Object> newAccountsMap = null;
        if (existedAccountMap == null) {
            newAccountsMap = new LinkedHashMap<>();
        } else {
            newAccountsMap = existedAccountMap;
        }

        if (StringUtils.isEmpty(plainAccessConfig.getAccessKey()) ||
            plainAccessConfig.getAccessKey().length() <= AclConstants.ACCESS_KEY_MIN_LENGTH) {
            throw new AclException(String.format(
                "The accessKey=%s cannot be null and length should longer than 6",
                plainAccessConfig.getAccessKey()));
        }
        newAccountsMap.put(AclConstants.CONFIG_ACCESS_KEY, plainAccessConfig.getAccessKey());

        if (!StringUtils.isEmpty(plainAccessConfig.getSecretKey())) {
            if (plainAccessConfig.getSecretKey().length() <= AclConstants.SECRET_KEY_MIN_LENGTH) {
                throw new AclException(String.format(
                    "The secretKey=%s value length should longer than 6",
                    plainAccessConfig.getSecretKey()));
            }
            newAccountsMap.put(AclConstants.CONFIG_SECRET_KEY, plainAccessConfig.getSecretKey());
        }
        if (plainAccessConfig.getWhiteRemoteAddress() != null) {
            newAccountsMap.put(AclConstants.CONFIG_WHITE_ADDR, plainAccessConfig.getWhiteRemoteAddress());
        }
        if (!StringUtils.isEmpty(String.valueOf(plainAccessConfig.isAdmin()))) {
            newAccountsMap.put(AclConstants.CONFIG_ADMIN_ROLE, plainAccessConfig.isAdmin());
        }
        if (!StringUtils.isEmpty(plainAccessConfig.getDefaultTopicPerm())) {
            newAccountsMap.put(AclConstants.CONFIG_DEFAULT_TOPIC_PERM, plainAccessConfig.getDefaultTopicPerm());
        }
        if (!StringUtils.isEmpty(plainAccessConfig.getDefaultGroupPerm())) {
            newAccountsMap.put(AclConstants.CONFIG_DEFAULT_GROUP_PERM, plainAccessConfig.getDefaultGroupPerm());
        }
        if (plainAccessConfig.getTopicPerms() != null) {
            newAccountsMap.put(AclConstants.CONFIG_TOPIC_PERMS, plainAccessConfig.getTopicPerms());
        }
        if (plainAccessConfig.getGroupPerms() != null) {
            newAccountsMap.put(AclConstants.CONFIG_GROUP_PERMS, plainAccessConfig.getGroupPerms());
        }
        if (plainAccessConfig.getResourcePerms() != null) {
            newAccountsMap.put(AclConstants.CONFIG_RESOURCE_PERMS, plainAccessConfig.getResourcePerms());
        }
        if (plainAccessConfig.getNamespacePerms() != null) {
            newAccountsMap.put(AclConstants.CONFIG_NAMESPACE_PERMS, plainAccessConfig.getNamespacePerms());
        }

        return newAccountsMap;
    }

    public boolean deleteAccessConfig(String accesskey) {
        if (StringUtils.isEmpty(accesskey)) {
            log.error("Parameter value accesskey is null or empty String,Please check your parameter");
            return false;
        }

        if (accessKeyTable.containsKey(accesskey)) {
            String aclFileName = accessKeyTable.get(accesskey);
            Map<String, Object> aclAccessConfigMap = AclUtils.getYamlDataObject(aclFileName,
                Map.class);
            if (aclAccessConfigMap == null || aclAccessConfigMap.isEmpty()) {
                log.warn("No data found in {} when deleting access config of {}", aclFileName, accesskey);
                return true;
            }
            List<Map<String, Object>> accounts = (List<Map<String, Object>>) aclAccessConfigMap.get("accounts");
            Iterator<Map<String, Object>> itemIterator = accounts.iterator();
            while (itemIterator.hasNext()) {
                if (itemIterator.next().get(AclConstants.CONFIG_ACCESS_KEY).equals(accesskey)) {
                    // Delete the related acl config element
                    itemIterator.remove();
                    accessKeyTable.remove(accesskey);
                    aclAccessConfigMap.put(AclConstants.CONFIG_ACCOUNTS, accounts);
                    return AclUtils.writeDataObject(aclFileName, updateAclConfigFileVersion(aclFileName, aclAccessConfigMap));
                }
            }
        }
        return false;
    }

    public boolean updateGlobalWhiteAddrsConfig(List<String> globalWhiteAddrsList) {
        return this.updateGlobalWhiteAddrsConfig(globalWhiteAddrsList, this.defaultAclFile);
    }

    public boolean updateGlobalWhiteAddrsConfig(List<String> globalWhiteAddrsList, String fileName) {
        if (fileName == null || fileName.equals("")) {
            fileName = this.defaultAclFile;
        }

        if (globalWhiteAddrsList == null) {
            log.error("Parameter value globalWhiteAddrsList is null,Please check your parameter");
            return false;
        }

        File file = new File(fileName);
        if (!file.exists() || file.isDirectory()) {
            log.error("Parameter value " + fileName + " is not exist or is a directory, please check your parameter");
            return false;
        }

        if (!fileName.startsWith(fileHome)) {
            log.error("Parameter value " + fileName + " is not in the directory rocketmq.home.dir " + fileHome);
            return false;
        }

        if (!fileName.endsWith(".yml") && fileName.endsWith(".yaml")) {
            log.error("Parameter value " + fileName + " is not a ACL configuration file");
            return false;
        }

        Map<String, Object> aclAccessConfigMap = AclUtils.getYamlDataObject(fileName, Map.class);
        if (aclAccessConfigMap == null) {
            aclAccessConfigMap = new HashMap<>();
            log.info("No data in {}, create a new aclAccessConfigMap", fileName);
        }
        // Update globalWhiteRemoteAddr element in memory map firstly
        aclAccessConfigMap.put(AclConstants.CONFIG_GLOBAL_WHITE_ADDRS, new ArrayList<>(globalWhiteAddrsList));
        return AclUtils.writeDataObject(fileName, updateAclConfigFileVersion(fileName, aclAccessConfigMap));

    }

    public AclConfig getAllAclConfig() {
        AclConfig aclConfig = new AclConfig();
        List<PlainAccessConfig> configs = new ArrayList<>();
        List<String> whiteAddrs = new ArrayList<>();
        Set<String> accessKeySets = new HashSet<>();

        for (int i = 0; i < fileList.size(); i++) {
            String path = fileList.get(i);
            JSONObject plainAclConfData = AclUtils.getYamlDataObject(path,
                JSONObject.class);
            if (plainAclConfData == null || plainAclConfData.isEmpty()) {
                continue;
            }
            JSONArray globalWhiteAddrs = plainAclConfData.getJSONArray(AclConstants.CONFIG_GLOBAL_WHITE_ADDRS);
            if (globalWhiteAddrs != null && !globalWhiteAddrs.isEmpty()) {
                whiteAddrs.addAll(globalWhiteAddrs.toJavaList(String.class));
            }

            JSONArray accounts = plainAclConfData.getJSONArray(AclConstants.CONFIG_ACCOUNTS);
            if (accounts != null && !accounts.isEmpty()) {
                List<PlainAccessConfig> plainAccessConfigs = accounts.toJavaList(PlainAccessConfig.class);
                for (int j = 0; j < plainAccessConfigs.size(); j++) {
                    if (!accessKeySets.contains(plainAccessConfigs.get(j).getAccessKey())) {
                        accessKeySets.add(plainAccessConfigs.get(j).getAccessKey());
                        PlainAccessConfig plainAccessConfig = new PlainAccessConfig();
                        plainAccessConfig.setGroupPerms(plainAccessConfigs.get(j).getGroupPerms());
                        plainAccessConfig.setDefaultTopicPerm(plainAccessConfigs.get(j).getDefaultTopicPerm());
                        plainAccessConfig.setDefaultGroupPerm(plainAccessConfigs.get(j).getDefaultGroupPerm());
                        plainAccessConfig.setAccessKey(plainAccessConfigs.get(j).getAccessKey());
                        plainAccessConfig.setSecretKey(plainAccessConfigs.get(j).getSecretKey());
                        plainAccessConfig.setAdmin(plainAccessConfigs.get(j).isAdmin());
                        plainAccessConfig.setTopicPerms(plainAccessConfigs.get(j).getTopicPerms());
                        plainAccessConfig.setWhiteRemoteAddress(plainAccessConfigs.get(j).getWhiteRemoteAddress());
                        plainAccessConfig.setResourcePerms(plainAccessConfigs.get(j).getResourcePerms());
                        plainAccessConfig.setNamespacePerms(plainAccessConfigs.get(j).getNamespacePerms());
                        configs.add(plainAccessConfig);
                    }
                }
            }
        }
        aclConfig.setPlainAccessConfigs(configs);
        aclConfig.setGlobalWhiteAddrs(whiteAddrs);
        return aclConfig;
    }

    private void watch() {
        try {
            AclFileWatchService aclFileWatchService = new AclFileWatchService(defaultAclDir, defaultAclFile, new AclFileWatchService.Listener() {
                @Override
                public void onFileChanged(String aclFileName) {
                    load(aclFileName);
                }

                @Override
                public void onFileNumChanged(String path) {
                    load();
                }
            });
            aclFileWatchService.start();
            log.info("Succeed to start AclFileWatchService");
            this.isWatchStart = true;
        } catch (Exception e) {
            log.error("Failed to start AclWatcherService", e);
        }

    }

    void checkPerm(PlainAccessResource needCheckedAccess, PlainAccessResource ownedAccess) {
        permissionChecker.check(needCheckedAccess, ownedAccess);
    }

    void clearPermissionInfo() {
        this.aclPlainAccessResourceMap.clear();
        this.accessKeyTable.clear();
        this.globalWhiteRemoteAddressStrategy.clear();
    }

    public void checkPlainAccessConfig(PlainAccessConfig plainAccessConfig) throws AclException {
        if (plainAccessConfig.getAccessKey() == null
            || plainAccessConfig.getSecretKey() == null
            || plainAccessConfig.getAccessKey().length() <= AclConstants.ACCESS_KEY_MIN_LENGTH
            || plainAccessConfig.getSecretKey().length() <= AclConstants.SECRET_KEY_MIN_LENGTH) {
            throw new AclException(String.format(
                "The accessKey=%s and secretKey=%s cannot be null and length should longer than 6",
                plainAccessConfig.getAccessKey(), plainAccessConfig.getSecretKey()));
        }
    }

    public void checkResourceAndPerm(ResourceAndPerm resourceAndPerm) throws AclException {
        if (resourceAndPerm.getResource() == null
            || resourceAndPerm.getType() == null
            || resourceAndPerm.getPerm() == null) {
            throw new AclException(String.format(
                "The resource=%s type=%s and perm=%s cannot be null",
                resourceAndPerm.getResource(), resourceAndPerm.getType(), resourceAndPerm.getPerm()));
        }
    }

    public PlainAccessResource buildPlainAccessResource(PlainAccessConfig plainAccessConfig) throws AclException {
        checkPlainAccessConfig(plainAccessConfig);
        return PlainAccessResource.build(plainAccessConfig, remoteAddressStrategyFactory.
            getRemoteAddressStrategy(plainAccessConfig.getWhiteRemoteAddress()));
    }

    public void validate(PlainAccessResource plainAccessResource) {

        // Check the global white remote addr
        for (RemoteAddressStrategy remoteAddressStrategy : globalWhiteRemoteAddressStrategy) {
            if (remoteAddressStrategy.match(plainAccessResource)) {
                return;
            }
        }

        if (plainAccessResource.getAccessKey() == null) {
            throw new AclException(String.format("No accessKey is configured"));
        }

        if (!accessKeyTable.containsKey(plainAccessResource.getAccessKey())) {
            throw new AclException(String.format("No acl config for %s", plainAccessResource.getAccessKey()));
        }

        // Check the white addr for accesskey
        String aclFileName = accessKeyTable.get(plainAccessResource.getAccessKey());
        PlainAccessResource ownedAccess = aclPlainAccessResourceMap.get(aclFileName).get(plainAccessResource.getAccessKey());
        if (null == ownedAccess) {
            throw new AclException(String.format("No PlainAccessResource for accessKey=%s", plainAccessResource.getAccessKey()));
        }
        if (ownedAccess.getRemoteAddressStrategy().match(plainAccessResource)) {
            return;
        }

        // Check the signature
        String signature = AclUtils.calSignature(plainAccessResource.getContent(), ownedAccess.getSecretKey());
        if (!signature.equals(plainAccessResource.getSignature())) {
            throw new AclException(String.format("Check signature failed for accessKey=%s", plainAccessResource.getAccessKey()));
        }

        //Skip the topic RMQ_SYS_TRACE_TOPIC permission check,if the topic RMQ_SYS_TRACE_TOPIC is used for message trace
        Map<String, Byte> resourcePermMap = plainAccessResource.getResourcePermMap();
        if (resourcePermMap != null) {
            Byte permission = resourcePermMap.get(TopicValidator.RMQ_SYS_TRACE_TOPIC);
            if (permission != null && permission == Permission.PUB) {
                return;
            }
        }

        // Check perm of each resource
        checkPerm(plainAccessResource, ownedAccess);
    }

    public boolean isWatchStart() {
        return isWatchStart;
    }
}
