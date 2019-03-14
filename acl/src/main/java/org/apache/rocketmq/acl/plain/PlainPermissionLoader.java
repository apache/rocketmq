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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.rocketmq.acl.common.AclException;
import org.apache.rocketmq.acl.common.AclUtils;
import org.apache.rocketmq.acl.common.Permission;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.srvutil.FileWatchService;

public class PlainPermissionLoader {

    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.COMMON_LOGGER_NAME);

    private static final String DEFAULT_PLAIN_ACL_FILE = "/conf/plain_acl.yml";

    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    private String fileHome = System.getProperty(MixAll.ROCKETMQ_HOME_PROPERTY,
        System.getenv(MixAll.ROCKETMQ_HOME_ENV));

    private String fileName = System.getProperty("rocketmq.acl.plain.file", DEFAULT_PLAIN_ACL_FILE);

    private  Map<String/** AccessKey **/, PlainAccessResource> plainAccessResourceMap = new HashMap<>();

    private  List<RemoteAddressStrategy> globalWhiteRemoteAddressStrategy = new ArrayList<>();

    private RemoteAddressStrategyFactory remoteAddressStrategyFactory = new RemoteAddressStrategyFactory();

    private boolean isWatchStart;

    public PlainPermissionLoader() {
        load();
        watch();
    }

    public void load() {

        Map<String, PlainAccessResource> plainAccessResourceMap = new HashMap<>();
        List<RemoteAddressStrategy> globalWhiteRemoteAddressStrategy = new ArrayList<>();

        JSONObject plainAclConfData = AclUtils.getYamlDataObject(fileHome + File.separator + fileName,
            JSONObject.class);

        if (plainAclConfData == null || plainAclConfData.isEmpty()) {
            throw new AclException(String.format("%s file  is not data", fileHome + File.separator + fileName));
        }
        log.info("Broker plain acl conf data is : ", plainAclConfData.toString());
        JSONArray globalWhiteRemoteAddressesList = plainAclConfData.getJSONArray("globalWhiteRemoteAddresses");
        if (globalWhiteRemoteAddressesList != null && !globalWhiteRemoteAddressesList.isEmpty()) {
            for (int i = 0; i < globalWhiteRemoteAddressesList.size(); i++) {
                globalWhiteRemoteAddressStrategy.add(remoteAddressStrategyFactory.
                        getRemoteAddressStrategy(globalWhiteRemoteAddressesList.getString(i)));
            }
        }

        JSONArray accounts = plainAclConfData.getJSONArray("accounts");
        if (accounts != null && !accounts.isEmpty()) {
            List<PlainAccessConfig> plainAccessConfigList = accounts.toJavaList(PlainAccessConfig.class);
            for (PlainAccessConfig plainAccessConfig : plainAccessConfigList) {
                PlainAccessResource plainAccessResource = buildPlainAccessResource(plainAccessConfig);
                plainAccessResourceMap.put(plainAccessResource.getAccessKey(),plainAccessResource);
            }
        }

        this.globalWhiteRemoteAddressStrategy = globalWhiteRemoteAddressStrategy;
        this.plainAccessResourceMap = plainAccessResourceMap;
    }

    private void watch() {
        try {
            String watchFilePath = fileHome + fileName;
            FileWatchService fileWatchService = new FileWatchService(new String[] {watchFilePath}, new FileWatchService.Listener() {
                @Override
                public void onChanged(String path) {
                    log.info("The plain acl yml changed, reload the context");
                    load();
                }
            });
            fileWatchService.start();
            log.info("Succeed to start AclWatcherService");
            this.isWatchStart = true;
        } catch (Exception e) {
            log.error("Failed to start AclWatcherService", e);
        }
    }

    void checkPerm(PlainAccessResource needCheckedAccess, PlainAccessResource ownedAccess) {
        if (Permission.needAdminPerm(needCheckedAccess.getRequestCode()) && !ownedAccess.isAdmin()) {
            throw new AclException(String.format("Need admin permission for request code=%d, but accessKey=%s is not", needCheckedAccess.getRequestCode(), ownedAccess.getAccessKey()));
        }
        Map<String, Byte> needCheckedPermMap = needCheckedAccess.getResourcePermMap();
        Map<String, Byte> ownedPermMap = ownedAccess.getResourcePermMap();

        if (needCheckedPermMap == null) {
            // If the needCheckedPermMap is null,then return
            return;
        }

        if (ownedPermMap == null && ownedAccess.isAdmin()) {
            // If the ownedPermMap is null and it is an admin user, then return
            return;
        }

        for (Map.Entry<String, Byte> needCheckedEntry : needCheckedPermMap.entrySet()) {
            String resource = needCheckedEntry.getKey();
            Byte neededPerm = needCheckedEntry.getValue();
            boolean isGroup = PlainAccessResource.isRetryTopic(resource);

            if (ownedPermMap == null || !ownedPermMap.containsKey(resource)) {
                // Check the default perm
                byte ownedPerm = isGroup ? ownedAccess.getDefaultGroupPerm() :
                    ownedAccess.getDefaultTopicPerm();
                if (!Permission.checkPermission(neededPerm, ownedPerm)) {
                    throw new AclException(String.format("No default permission for %s", PlainAccessResource.printStr(resource, isGroup)));
                }
                continue;
            }
            if (!Permission.checkPermission(neededPerm, ownedPermMap.get(resource))) {
                throw new AclException(String.format("No default permission for %s", PlainAccessResource.printStr(resource, isGroup)));
            }
        }
    }

    void clearPermissionInfo() {
        this.plainAccessResourceMap.clear();
        this.globalWhiteRemoteAddressStrategy.clear();
    }

    public PlainAccessResource buildPlainAccessResource(PlainAccessConfig plainAccessConfig) throws AclException {
        if (plainAccessConfig.getAccessKey() == null
            || plainAccessConfig.getSecretKey() == null
            || plainAccessConfig.getAccessKey().length() <= 6
            || plainAccessConfig.getSecretKey().length() <= 6) {
            throw new AclException(String.format(
                "The accessKey=%s and secretKey=%s cannot be null and length should longer than 6",
                    plainAccessConfig.getAccessKey(), plainAccessConfig.getSecretKey()));
        }
        PlainAccessResource plainAccessResource = new PlainAccessResource();
        plainAccessResource.setAccessKey(plainAccessConfig.getAccessKey());
        plainAccessResource.setSecretKey(plainAccessConfig.getSecretKey());
        plainAccessResource.setWhiteRemoteAddress(plainAccessConfig.getWhiteRemoteAddress());

        plainAccessResource.setAdmin(plainAccessConfig.isAdmin());

        plainAccessResource.setDefaultGroupPerm(Permission.parsePermFromString(plainAccessConfig.getDefaultGroupPerm()));
        plainAccessResource.setDefaultTopicPerm(Permission.parsePermFromString(plainAccessConfig.getDefaultTopicPerm()));

        Permission.parseResourcePerms(plainAccessResource, false, plainAccessConfig.getGroupPerms());
        Permission.parseResourcePerms(plainAccessResource, true, plainAccessConfig.getTopicPerms());

        plainAccessResource.setRemoteAddressStrategy(remoteAddressStrategyFactory.
                getRemoteAddressStrategy(plainAccessResource.getWhiteRemoteAddress()));

        return plainAccessResource;
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

        if (!plainAccessResourceMap.containsKey(plainAccessResource.getAccessKey())) {
            throw new AclException(String.format("No acl config for %s", plainAccessResource.getAccessKey()));
        }

        // Check the white addr for accesskey
        PlainAccessResource ownedAccess = plainAccessResourceMap.get(plainAccessResource.getAccessKey());
        if (ownedAccess.getRemoteAddressStrategy().match(plainAccessResource)) {
            return;
        }

        // Check the signature
        String signature = AclUtils.calSignature(plainAccessResource.getContent(), ownedAccess.getSecretKey());
        if (!signature.equals(plainAccessResource.getSignature())) {
            throw new AclException(String.format("Check signature failed for accessKey=%s", plainAccessResource.getAccessKey()));
        }
        // Check perm of each resource

        checkPerm(plainAccessResource, ownedAccess);
    }

    public boolean isWatchStart() {
        return isWatchStart;
    }

    static class PlainAccessConfig {

        private String accessKey;

        private String secretKey;

        private String whiteRemoteAddress;

        private boolean admin;

        private String defaultTopicPerm;

        private String defaultGroupPerm;

        private List<String> topicPerms;

        private List<String> groupPerms;

        public String getAccessKey() {
            return accessKey;
        }

        public void setAccessKey(String accessKey) {
            this.accessKey = accessKey;
        }

        public String getSecretKey() {
            return secretKey;
        }

        public void setSecretKey(String secretKey) {
            this.secretKey = secretKey;
        }

        public String getWhiteRemoteAddress() {
            return whiteRemoteAddress;
        }

        public void setWhiteRemoteAddress(String whiteRemoteAddress) {
            this.whiteRemoteAddress = whiteRemoteAddress;
        }

        public boolean isAdmin() {
            return admin;
        }

        public void setAdmin(boolean admin) {
            this.admin = admin;
        }

        public String getDefaultTopicPerm() {
            return defaultTopicPerm;
        }

        public void setDefaultTopicPerm(String defaultTopicPerm) {
            this.defaultTopicPerm = defaultTopicPerm;
        }

        public String getDefaultGroupPerm() {
            return defaultGroupPerm;
        }

        public void setDefaultGroupPerm(String defaultGroupPerm) {
            this.defaultGroupPerm = defaultGroupPerm;
        }

        public List<String> getTopicPerms() {
            return topicPerms;
        }

        public void setTopicPerms(List<String> topicPerms) {
            this.topicPerms = topicPerms;
        }

        public List<String> getGroupPerms() {
            return groupPerms;
        }

        public void setGroupPerms(List<String> groupPerms) {
            this.groupPerms = groupPerms;
        }

    }

}
