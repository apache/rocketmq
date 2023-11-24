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
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.PlainAccessConfig;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.topic.TopicValidator;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.protocol.DataVersion;
import org.apache.rocketmq.srvutil.AclFileWatchService;

public class PlainPermissionManager {

    private static final Logger log = LoggerFactory.getLogger(LoggerName.COMMON_LOGGER_NAME);

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
        List<String> allAclFileFullPath = new ArrayList<>();
        File rootFile = new File(path);
        File[] files = rootFile.listFiles();
        if (!rootFile.exists() || files == null) {
            log.info("The default acl dir {} is not exist", path);
            return allAclFileFullPath;
        }
        for (File file : files) {
            String fileName = file.getAbsolutePath();
            if ((fileHome + MixAll.ACL_CONF_TOOLS_FILE).equals(fileName)) {
                continue;
            }
            if (fileName.endsWith(".yml") || fileName.endsWith(".yaml")) {
                allAclFileFullPath.add(fileName);
            } else if (file.isDirectory()) {
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
            PlainAccessData plainAclConfData = AclUtils.getYamlDataObject(currentFile,
                PlainAccessData.class);
            if (plainAclConfData == null) {
                log.warn("No data in file {}", currentFile);
                continue;
            }
            log.info("Broker plain acl conf data is : {}", plainAclConfData.toString());

            List<RemoteAddressStrategy> globalWhiteRemoteAddressStrategyList = new ArrayList<>();
            List<String> globalWhiteRemoteAddressesList = plainAclConfData.getGlobalWhiteRemoteAddresses();
            if (globalWhiteRemoteAddressesList != null && !globalWhiteRemoteAddressesList.isEmpty()) {
                for (int j = 0; j < globalWhiteRemoteAddressesList.size(); j++) {
                    globalWhiteRemoteAddressStrategyList.add(remoteAddressStrategyFactory.
                        getRemoteAddressStrategy(globalWhiteRemoteAddressesList.get(j)));
                }
            }
            if (globalWhiteRemoteAddressStrategyList.size() > 0) {
                globalWhiteRemoteAddressStrategyMap.put(currentFile, globalWhiteRemoteAddressStrategyList);
                globalWhiteRemoteAddressStrategy.addAll(globalWhiteRemoteAddressStrategyList);
            }

            List<PlainAccessConfig> accounts = plainAclConfData.getAccounts();
            Map<String, PlainAccessResource> plainAccessResourceMap = new HashMap<>();
            if (accounts != null && !accounts.isEmpty()) {
                for (PlainAccessConfig plainAccessConfig : accounts) {
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

            List<PlainAccessData.DataVersion> dataVersions = plainAclConfData.getDataVersion();
            DataVersion dataVersion = new DataVersion();
            if (dataVersions != null && !dataVersions.isEmpty()) {
                DataVersion firstElement = new DataVersion();
                firstElement.setCounter(new AtomicLong(dataVersions.get(0).getCounter()));
                firstElement.setTimestamp(dataVersions.get(0).getTimestamp());
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

        PlainAccessData plainAclConfData = AclUtils.getYamlDataObject(aclFilePath,
            PlainAccessData.class);
        if (plainAclConfData == null) {
            log.warn("No data in {}, skip it", aclFilePath);
            return;
        }
        log.info("Broker plain acl conf data is : {}", plainAclConfData.toString());
        List<String> globalWhiteRemoteAddressesList = plainAclConfData.getGlobalWhiteRemoteAddresses();
        if (globalWhiteRemoteAddressesList != null && !globalWhiteRemoteAddressesList.isEmpty()) {
            for (int i = 0; i < globalWhiteRemoteAddressesList.size(); i++) {
                globalWhiteRemoteAddressStrategy.add(remoteAddressStrategyFactory.
                    getRemoteAddressStrategy(globalWhiteRemoteAddressesList.get(i)));
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

        List<PlainAccessConfig> accounts = plainAclConfData.getAccounts();
        if (accounts != null && !accounts.isEmpty()) {
            for (PlainAccessConfig plainAccessConfig : accounts) {
                PlainAccessResource plainAccessResource = buildPlainAccessResource(plainAccessConfig);
                //AccessKey can not be defined in multiple ACL files
                String oldPath = this.accessKeyTable.get(plainAccessResource.getAccessKey());
                if (oldPath == null || aclFilePath.equals(oldPath)) {
                    plainAccessResourceMap.put(plainAccessResource.getAccessKey(), plainAccessResource);
                    this.accessKeyTable.put(plainAccessResource.getAccessKey(), aclFilePath);
                } else {
                    log.warn("The accessKey {} is repeated in multiple ACL files", plainAccessResource.getAccessKey());
                }
            }
        }

        // For loading dataversion part just
        List<PlainAccessData.DataVersion> dataVersions = plainAclConfData.getDataVersion();
        DataVersion dataVersion = new DataVersion();
        if (dataVersions != null && !dataVersions.isEmpty()) {
            DataVersion firstElement = new DataVersion();
            firstElement.setCounter(new AtomicLong(dataVersions.get(0).getCounter()));
            firstElement.setTimestamp(dataVersions.get(0).getTimestamp());
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

    public PlainAccessData updateAclConfigFileVersion(String aclFileName, PlainAccessData updateAclConfigMap) {

        List<PlainAccessData.DataVersion> dataVersions = updateAclConfigMap.getDataVersion();
        DataVersion dataVersion = new DataVersion();
        if (dataVersions != null) {
            if (dataVersions.size() > 0) {
                dataVersion.setTimestamp(dataVersions.get(0).getTimestamp());
                dataVersion.setCounter(new AtomicLong(dataVersions.get(0).getCounter()));
            }
        }
        dataVersion.nextVersion();
        List<PlainAccessData.DataVersion> versionElement = new ArrayList<>();
        PlainAccessData.DataVersion dataVersionNew = new PlainAccessData.DataVersion();
        dataVersionNew.setTimestamp(dataVersion.getTimestamp());
        dataVersionNew.setCounter(dataVersion.getCounter().get());
        versionElement.add(dataVersionNew);
        updateAclConfigMap.setDataVersion(versionElement);

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
            PlainAccessConfig updateAccountMap = null;
            String aclFileName = accessKeyTable.get(plainAccessConfig.getAccessKey());
            PlainAccessData aclAccessConfigMap = AclUtils.getYamlDataObject(aclFileName, PlainAccessData.class);
            List<PlainAccessConfig> accounts = aclAccessConfigMap.getAccounts();
            if (null != accounts) {
                for (PlainAccessConfig account : accounts) {
                    if (account.getAccessKey().equals(plainAccessConfig.getAccessKey())) {
                        // Update acl access config elements
                        accounts.remove(account);
                        updateAccountMap = createAclAccessConfigMap(account, plainAccessConfig);
                        accounts.add(updateAccountMap);
                        aclAccessConfigMap.setAccounts(accounts);
                        break;
                    }
                }
            } else {
                // Maybe deleted in file, add it back
                accounts = new LinkedList<>();
                updateAccountMap = createAclAccessConfigMap(null, plainAccessConfig);
                accounts.add(updateAccountMap);
                aclAccessConfigMap.setAccounts(accounts);
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
            PlainAccessData aclAccessConfigMap = AclUtils.getYamlDataObject(defaultAclFile, PlainAccessData.class);
            if (aclAccessConfigMap == null) {
                aclAccessConfigMap = new PlainAccessData();
            }
            List<PlainAccessConfig> accounts = aclAccessConfigMap.getAccounts();
            // When no accounts defined
            if (null == accounts) {
                accounts = new ArrayList<>();
            }
            accounts.add(createAclAccessConfigMap(null, plainAccessConfig));
            aclAccessConfigMap.setAccounts(accounts);
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

    public PlainAccessConfig createAclAccessConfigMap(PlainAccessConfig existedAccountMap,
        PlainAccessConfig plainAccessConfig) {

        PlainAccessConfig newAccountsMap = null;
        if (existedAccountMap == null) {
            newAccountsMap = new PlainAccessConfig();
        } else {
            newAccountsMap = existedAccountMap;
        }

        if (StringUtils.isEmpty(plainAccessConfig.getAccessKey()) ||
            plainAccessConfig.getAccessKey().length() <= AclConstants.ACCESS_KEY_MIN_LENGTH) {
            throw new AclException(String.format(
                "The accessKey=%s cannot be null and length should longer than 6",
                plainAccessConfig.getAccessKey()));
        }
        newAccountsMap.setAccessKey(plainAccessConfig.getAccessKey());

        if (!StringUtils.isEmpty(plainAccessConfig.getSecretKey())) {
            if (plainAccessConfig.getSecretKey().length() <= AclConstants.SECRET_KEY_MIN_LENGTH) {
                throw new AclException(String.format(
                    "The secretKey=%s value length should longer than 6",
                    plainAccessConfig.getSecretKey()));
            }
            newAccountsMap.setSecretKey(plainAccessConfig.getSecretKey());
        }
        if (plainAccessConfig.getWhiteRemoteAddress() != null) {
            newAccountsMap.setWhiteRemoteAddress(plainAccessConfig.getWhiteRemoteAddress());
        }
        if (!StringUtils.isEmpty(String.valueOf(plainAccessConfig.isAdmin()))) {
            newAccountsMap.setAdmin(plainAccessConfig.isAdmin());
        }
        if (!StringUtils.isEmpty(plainAccessConfig.getDefaultTopicPerm())) {
            newAccountsMap.setDefaultTopicPerm(plainAccessConfig.getDefaultTopicPerm());
        }
        if (!StringUtils.isEmpty(plainAccessConfig.getDefaultGroupPerm())) {
            newAccountsMap.setDefaultGroupPerm(plainAccessConfig.getDefaultGroupPerm());
        }
        if (plainAccessConfig.getTopicPerms() != null) {
            newAccountsMap.setTopicPerms(plainAccessConfig.getTopicPerms());
        }
        if (plainAccessConfig.getGroupPerms() != null) {
            newAccountsMap.setGroupPerms(plainAccessConfig.getGroupPerms());
        }

        return newAccountsMap;
    }

    public boolean deleteAccessConfig(String accessKey) {
        if (StringUtils.isEmpty(accessKey)) {
            log.error("Parameter value accessKey is null or empty String,Please check your parameter");
            return false;
        }

        if (accessKeyTable.containsKey(accessKey)) {
            String aclFileName = accessKeyTable.get(accessKey);
            PlainAccessData aclAccessConfigData = AclUtils.getYamlDataObject(aclFileName,
                PlainAccessData.class);
            if (aclAccessConfigData == null) {
                log.warn("No data found in {} when deleting access config of {}", aclFileName, accessKey);
                return true;
            }
            List<PlainAccessConfig> accounts = aclAccessConfigData.getAccounts();
            Iterator<PlainAccessConfig> itemIterator = accounts.iterator();
            while (itemIterator.hasNext()) {
                if (itemIterator.next().getAccessKey().equals(accessKey)) {
                    // Delete the related acl config element
                    itemIterator.remove();
                    accessKeyTable.remove(accessKey);
                    aclAccessConfigData.setAccounts(accounts);
                    return AclUtils.writeDataObject(aclFileName, updateAclConfigFileVersion(aclFileName, aclAccessConfigData));
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

        if (!file.getAbsolutePath().startsWith(fileHome)) {
            log.error("Parameter value " + fileName + " is not in the directory rocketmq.home.dir " + fileHome);
            return false;
        }

        if (!fileName.endsWith(".yml") && fileName.endsWith(".yaml")) {
            log.error("Parameter value " + fileName + " is not a ACL configuration file");
            return false;
        }

        PlainAccessData aclAccessConfigMap = AclUtils.getYamlDataObject(fileName, PlainAccessData.class);
        if (aclAccessConfigMap == null) {
            aclAccessConfigMap = new PlainAccessData();
            log.info("No data in {}, create a new aclAccessConfigMap", fileName);
        }
        // Update globalWhiteRemoteAddr element in memory map firstly
        aclAccessConfigMap.setGlobalWhiteRemoteAddresses(new ArrayList<>(globalWhiteAddrsList));
        return AclUtils.writeDataObject(fileName, updateAclConfigFileVersion(fileName, aclAccessConfigMap));

    }

    public AclConfig getAllAclConfig() {
        AclConfig aclConfig = new AclConfig();
        List<PlainAccessConfig> configs = new ArrayList<>();
        List<String> whiteAddrs = new ArrayList<>();
        Set<String> accessKeySets = new HashSet<>();

        for (int i = 0; i < fileList.size(); i++) {
            String path = fileList.get(i);
            PlainAccessData plainAclConfData = AclUtils.getYamlDataObject(path,
                PlainAccessData.class);
            if (plainAclConfData == null) {
                continue;
            }
            List<String> globalWhiteAddrs = plainAclConfData.getGlobalWhiteRemoteAddresses();
            if (globalWhiteAddrs != null && !globalWhiteAddrs.isEmpty()) {
                whiteAddrs.addAll(globalWhiteAddrs);
            }

            List<PlainAccessConfig> plainAccessConfigs = plainAclConfData.getAccounts();
            if (plainAccessConfigs != null && !plainAccessConfigs.isEmpty()) {
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
            throw new AclException("No accessKey is configured");
        }

        if (!accessKeyTable.containsKey(plainAccessResource.getAccessKey())) {
            throw new AclException(String.format("No acl config for %s", plainAccessResource.getAccessKey()));
        }

        // Check the white addr for accessKey
        String aclFileName = accessKeyTable.get(plainAccessResource.getAccessKey());
        PlainAccessResource ownedAccess = aclPlainAccessResourceMap.getOrDefault(aclFileName, new HashMap<>()).get(plainAccessResource.getAccessKey());
        if (ownedAccess == null) {
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
