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
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.acl.common.AclException;
import org.apache.rocketmq.acl.common.AclUtils;
import org.apache.rocketmq.acl.common.Permission;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;

public class PlainPermissionLoader {

    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.ACL_PLUG_LOGGER_NAME);

    private static final String DEFAULT_PLAIN_ACL_FILE = "/conf/plain_acl.yml";

    private String fileHome = System.getProperty(MixAll.ROCKETMQ_HOME_PROPERTY,
        System.getenv(MixAll.ROCKETMQ_HOME_ENV));

    private String fileName = System.getProperty("rocketmq.acl.plain.file", DEFAULT_PLAIN_ACL_FILE);

    private Map<String/** AccessKey **/, PlainAccessResource> plainAccessResourceMap = new HashMap<>();

    private List<RemoteAddressStrategy> globalWhiteRemoteAddressStrategy = new ArrayList<>();

    private RemoteAddressStrategyFactory remoteAddressStrategyFactory = new RemoteAddressStrategyFactory();

    private boolean isWatchStart;

    public PlainPermissionLoader() {
        initialize();
        watch();
    }

    public void initialize() {
        JSONObject accessControlTransport = AclUtils.getYamlDataObject(fileHome + fileName,
            JSONObject.class);

        if (accessControlTransport == null || accessControlTransport.isEmpty()) {
            throw new AclException(String.format("%s file  is not data", fileHome + fileName));
        }
        log.info("BorkerAccessControlTransport data is : ", accessControlTransport.toString());
        JSONArray globalWhiteRemoteAddressesList = accessControlTransport.getJSONArray("globalWhiteRemoteAddresses");
        if (globalWhiteRemoteAddressesList != null && !globalWhiteRemoteAddressesList.isEmpty()) {
            for (int i = 0; i < globalWhiteRemoteAddressesList.size(); i++) {
                addGlobalWhiteRemoteAddress(globalWhiteRemoteAddressesList.getString(i));
            }
        }

        JSONArray accounts = accessControlTransport.getJSONArray("accounts");
        if (accounts != null && !accounts.isEmpty()) {
            List<PlainAccessConfig> plainAccessList = accounts.toJavaList(PlainAccessConfig.class);
            for (PlainAccessConfig plainAccess : plainAccessList) {
                this.addPlainAccessResource(getPlainAccessResource(plainAccess));
            }
        }
    }

    private void watch() {
        String version = System.getProperty("java.version");
        String[] str = StringUtils.split(version, ".");
        if (Integer.valueOf(str[1]) < 7) {
            log.warn("Watch need jdk equal or greater than 1.7, current version is {}", str[1]);
            return;
        }

        try {
            int fileIndex = fileName.lastIndexOf("/") + 1;
            String watchDirectory = fileName.substring(0, fileIndex);
            final String watchFileName = fileName.substring(fileIndex);
            log.info("watch directory is {} , watch directory file name is {} ", fileHome + watchDirectory, watchFileName);

            final WatchService watcher = FileSystems.getDefault().newWatchService();
            Path p = Paths.get(fileHome + watchDirectory);
            p.register(watcher, StandardWatchEventKinds.ENTRY_MODIFY, StandardWatchEventKinds.ENTRY_CREATE);
            ServiceThread watcherServcie = new ServiceThread() {

                public void run() {
                    while (true) {
                        try {
                            WatchKey watchKey = watcher.take();
                            List<WatchEvent<?>> watchEvents = watchKey.pollEvents();
                            for (WatchEvent<?> event : watchEvents) {
                                if (watchFileName.equals(event.context().toString())
                                    && (StandardWatchEventKinds.ENTRY_MODIFY.equals(event.kind())
                                    || StandardWatchEventKinds.ENTRY_CREATE.equals(event.kind()))) {
                                    log.info("{} make a difference  change is : {}", watchFileName, event.toString());
                                    PlainPermissionLoader.this.clearPermissionInfo();
                                    initialize();
                                }
                            }
                            watchKey.reset();
                        } catch (InterruptedException e) {
                            log.error(e.getMessage(), e);
                            UtilAll.sleep(3000);

                        }
                    }
                }

                @Override
                public String getServiceName() {
                    return "AclWatcherService";
                }

            };
            watcherServcie.start();
            log.info("Succeed to start AclWatcherService");
            this.isWatchStart = true;
        } catch (IOException e) {
            log.error("Failed to start AclWatcherService", e);
        }
    }

    PlainAccessResource getPlainAccessResource(PlainAccessConfig plainAccess) {
        PlainAccessResource plainAccessResource = new PlainAccessResource();
        plainAccessResource.setAccessKey(plainAccess.getAccessKey());
        plainAccessResource.setSecretKey(plainAccess.getSecretKey());
        plainAccessResource.setWhiteRemoteAddress(plainAccess.getWhiteRemoteAddress());

        plainAccessResource.setAdmin(plainAccess.isAdmin());

        plainAccessResource.setDefaultGroupPerm(Permission.parsePermFromString(plainAccess.getDefaultGroupPerm()));
        plainAccessResource.setDefaultTopicPerm(Permission.parsePermFromString(plainAccess.getDefaultTopicPerm()));

        Permission.parseResourcePerms(plainAccessResource, false, plainAccess.getGroupPerms());
        Permission.parseResourcePerms(plainAccessResource, true, plainAccess.getTopicPerms());
        return plainAccessResource;
    }

    void checkPerm(PlainAccessResource needCheckedAccess, PlainAccessResource ownedAccess) {
        if (Permission.needAdminPerm(needCheckedAccess.getRequestCode()) && !ownedAccess.isAdmin()) {
            throw new AclException(String.format("Need admin permission for request code=%d, but accessKey=%s is not", needCheckedAccess.getRequestCode(), ownedAccess.getAccessKey()));
        }
        Map<String, Byte> needCheckedPermMap = needCheckedAccess.getResourcePermMap();
        Map<String, Byte> ownedPermMap = ownedAccess.getResourcePermMap();

        if (needCheckedPermMap == null) {
            //if the needCheckedPermMap is null,then return
            return;
        }

        for (Map.Entry<String, Byte> needCheckedEntry : needCheckedPermMap.entrySet()) {
            String resource = needCheckedEntry.getKey();
            Byte neededPerm = needCheckedEntry.getValue();
            boolean isGroup = PlainAccessResource.isRetryTopic(resource);

            if (!ownedPermMap.containsKey(resource)) {
                //Check the default perm
                byte ownedPerm = isGroup ? needCheckedAccess.getDefaultGroupPerm() :
                    needCheckedAccess.getDefaultTopicPerm();
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

    public void addPlainAccessResource(PlainAccessResource plainAccessResource) throws AclException {
        if (plainAccessResource.getAccessKey() == null
            || plainAccessResource.getSecretKey() == null
            || plainAccessResource.getAccessKey().length() <= 6
            || plainAccessResource.getSecretKey().length() <= 6) {
            throw new AclException(String.format(
                "The accessKey=%s and secretKey=%s cannot be null and length should longer than 6",
                plainAccessResource.getAccessKey(), plainAccessResource.getSecretKey()));
        }
        try {
            RemoteAddressStrategy remoteAddressStrategy = remoteAddressStrategyFactory
                .getRemoteAddressStrategy(plainAccessResource);
            plainAccessResource.setRemoteAddressStrategy(remoteAddressStrategy);

            if (plainAccessResourceMap.containsKey(plainAccessResource.getAccessKey())) {
                log.warn("Duplicate acl config  for {}, the newly one may overwrite the old", plainAccessResource.getAccessKey());
            }
            plainAccessResourceMap.put(plainAccessResource.getAccessKey(), plainAccessResource);
        } catch (Exception e) {
            throw new AclException(String.format("Load plain access resource failed %s  %s", e.getMessage(), plainAccessResource.toString()), e);
        }
    }

    private void addGlobalWhiteRemoteAddress(String remoteAddresses) {
        globalWhiteRemoteAddressStrategy.add(remoteAddressStrategyFactory.getRemoteAddressStrategy(remoteAddresses));
    }

    public void validate(PlainAccessResource plainAccessResource) {

        //Step 1, check the global white remote addr
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

        //Step 2, check the white addr for accesskey
        PlainAccessResource ownedAccess = plainAccessResourceMap.get(plainAccessResource.getAccessKey());
        if (ownedAccess.getRemoteAddressStrategy().match(plainAccessResource)) {
            return;
        }

        //Step 3, check the signature
        String signature = AclUtils.calSignature(plainAccessResource.getContent(), ownedAccess.getSecretKey());
        if (!signature.equals(plainAccessResource.getSignature())) {
            throw new AclException(String.format("Check signature failed for accessKey=%s", plainAccessResource.getAccessKey()));
        }
        //Step 4, check perm of each resource

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
