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
package org.apache.rocketmq.auth.migration.v1;

import org.apache.rocketmq.acl.common.AclException;
import org.apache.rocketmq.acl.common.AclUtils;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class PlainPermissionManager {

    private static final Logger log = LoggerFactory.getLogger(LoggerName.COMMON_LOGGER_NAME);

    private String fileHome = System.getProperty(MixAll.ROCKETMQ_HOME_PROPERTY,
        System.getenv(MixAll.ROCKETMQ_HOME_ENV));

    private String defaultAclDir;

    private String defaultAclFile;

    private List<String> fileList = new ArrayList<>();


    public PlainPermissionManager() {
        this.defaultAclDir = MixAll.dealFilePath(fileHome + File.separator + "conf" + File.separator + "acl");
        this.defaultAclFile = MixAll.dealFilePath(fileHome + File.separator + System.getProperty("rocketmq.acl.plain.file", "conf" + File.separator + "plain_acl.yml"));
        load();
    }

    public List<String> getAllAclFiles(String path) {
        if (!new File(path).exists()) {
            log.info("The default acl dir {} is not exist", path);
            return new ArrayList<>();
        }
        List<String> allAclFileFullPath = new ArrayList<>();
        File file = new File(path);
        File[] files = file.listFiles();
        for (int i = 0; files != null && i < files.length; i++) {
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

        assureAclConfigFilesExist();

        fileList = getAllAclFiles(defaultAclDir);
        if (new File(defaultAclFile).exists() && !fileList.contains(defaultAclFile)) {
            fileList.add(defaultAclFile);
        }
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

    public AclConfig getAllAclConfig() {
        AclConfig aclConfig = new AclConfig();
        List<PlainAccessConfig> configs = new ArrayList<>();
        List<String> whiteAddrs = new ArrayList<>();
        Set<String> accessKeySets = new HashSet<>();

        for (String path : fileList) {
            PlainAccessData plainAclConfData = AclUtils.getYamlDataObject(path, PlainAccessData.class);
            if (plainAclConfData == null) {
                continue;
            }
            List<String> globalWhiteAddrs = plainAclConfData.getGlobalWhiteRemoteAddresses();
            if (globalWhiteAddrs != null && !globalWhiteAddrs.isEmpty()) {
                whiteAddrs.addAll(globalWhiteAddrs);
            }

            List<PlainAccessConfig> plainAccessConfigs = plainAclConfData.getAccounts();
            if (plainAccessConfigs != null && !plainAccessConfigs.isEmpty()) {
                for (PlainAccessConfig accessConfig : plainAccessConfigs) {
                    if (!accessKeySets.contains(accessConfig.getAccessKey())) {
                        accessKeySets.add(accessConfig.getAccessKey());
                        PlainAccessConfig plainAccessConfig = new PlainAccessConfig();
                        plainAccessConfig.setGroupPerms(accessConfig.getGroupPerms());
                        plainAccessConfig.setDefaultTopicPerm(accessConfig.getDefaultTopicPerm());
                        plainAccessConfig.setDefaultGroupPerm(accessConfig.getDefaultGroupPerm());
                        plainAccessConfig.setAccessKey(accessConfig.getAccessKey());
                        plainAccessConfig.setSecretKey(accessConfig.getSecretKey());
                        plainAccessConfig.setAdmin(accessConfig.isAdmin());
                        plainAccessConfig.setTopicPerms(accessConfig.getTopicPerms());
                        plainAccessConfig.setWhiteRemoteAddress(accessConfig.getWhiteRemoteAddress());
                        configs.add(plainAccessConfig);
                    }
                }
            }
        }
        aclConfig.setPlainAccessConfigs(configs);
        aclConfig.setGlobalWhiteAddrs(whiteAddrs);
        return aclConfig;
    }
}
