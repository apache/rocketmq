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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.acl.common.AclException;
import org.apache.rocketmq.acl.common.AclUtils;
import org.apache.rocketmq.acl.common.Permission;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;

public class PlainPermissionLoader {

    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.ACL_PLUG_LOGGER_NAME);

    private String fileHome = System.getProperty(MixAll.ROCKETMQ_HOME_PROPERTY,
        System.getenv(MixAll.ROCKETMQ_HOME_ENV));

    private String fileName = System.getProperty("romcketmq.acl.plain.fileName", "/conf/transport.yml");

    private Map<String/** account **/
        , List<PlainAccessResource>> plainAccessResourceMap = new HashMap<>();

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
            throw new AclException("transport.yml file  is not data");
        }
        log.info("BorkerAccessControlTransport data is : ", accessControlTransport.toString());
        JSONArray globalWhiteRemoteAddressesList = accessControlTransport.getJSONArray("globalWhiteRemoteAddresses");
        if (globalWhiteRemoteAddressesList != null && !globalWhiteRemoteAddressesList.isEmpty()) {
            for (int i = 0; i < globalWhiteRemoteAddressesList.size(); i++) {
                setGlobalWhite(globalWhiteRemoteAddressesList.getString(i));
            }
        }

        JSONArray accounts = accessControlTransport.getJSONArray("accounts");
        if (accounts != null && !accounts.isEmpty()) {
            for (int i = 0; i < accounts.size(); i++) {
                this.setPlainAccessResource(getPlainAccessResource(accounts.getJSONObject(i)));
            }
        }
    }

    private void watch() {
        String version = System.getProperty("java.version");
        log.info("java.version is : {}", version);
        String[] str = StringUtils.split(version, ".");
        if (Integer.valueOf(str[1]) < 7) {
            log.warn("wacth need jdk 1.7 support , current version no support");
            return;
        }
        try {
            final WatchService watcher = FileSystems.getDefault().newWatchService();
            Path p = Paths.get(fileHome + "/conf/");
            p.register(watcher, StandardWatchEventKinds.ENTRY_MODIFY, StandardWatchEventKinds.ENTRY_CREATE);
            ServiceThread watcherServcie = new ServiceThread() {

                public void run() {
                    while (true) {
                        try {
                            while (true) {
                                WatchKey watchKey = watcher.take();
                                List<WatchEvent<?>> watchEvents = watchKey.pollEvents();
                                for (WatchEvent<?> event : watchEvents) {
                                    if ("transport.yml".equals(event.context().toString())
                                        && (StandardWatchEventKinds.ENTRY_MODIFY.equals(event.kind())
                                        || StandardWatchEventKinds.ENTRY_CREATE.equals(event.kind()))) {
                                        log.info("transprot.yml make a difference  change is : ", event.toString());
                                        PlainPermissionLoader.this.cleanAuthenticationInfo();
                                        initialize();
                                    }
                                }
                                watchKey.reset();
                            }
                        } catch (InterruptedException e) {
                            log.error(e.getMessage(), e);
                        }
                    }
                }

                @Override
                public String getServiceName() {
                    return "watcherServcie";
                }

            };
            watcherServcie.start();
            log.info("succeed start watcherServcie");
            this.isWatchStart = true;
        } catch (IOException e) {
            log.error(e.getMessage(), e);
        }
    }

    PlainAccessResource getPlainAccessResource(JSONObject account) {
        PlainAccessResource plainAccessResource = new PlainAccessResource();
        plainAccessResource.setAccessKey(account.getString("accessKey"));
        plainAccessResource.setSecretKey(account.getString("secretKey"));
        plainAccessResource.setWhiteRemoteAddress(account.getString("whiteRemoteAddress"));

        plainAccessResource.setAdmin(account.containsKey("admin") ? account.getBoolean("admin") : false);

        plainAccessResource.setDefaultGroupPerm(Permission.fromStringGetPermission(account.getString("defaultGroupPerm")));
        plainAccessResource.setDefaultTopicPerm(Permission.fromStringGetPermission(account.getString("defaultTopicPerm")));

        Permission.setTopicPerm(plainAccessResource, true, account.getJSONArray("groups"));
        Permission.setTopicPerm(plainAccessResource, true, account.getJSONArray("topics"));
        return plainAccessResource;
    }

    void checkPerm(PlainAccessResource needCheckplainAccessResource, PlainAccessResource plainAccessResource) {
        if (!plainAccessResource.isAdmin() && Permission.checkAdminCode(needCheckplainAccessResource.getRequestCode())) {
            throw new AclException(String.format("accessKey is %s  remoteAddress is %s , is not admin Premission . RequestCode is %d", plainAccessResource.getAccessKey(), plainAccessResource.getWhiteRemoteAddress(), needCheckplainAccessResource.getRequestCode()));
        }
        Map<String, Byte> needCheckTopicAndGourpPerm = needCheckplainAccessResource.getResourcePermMap();
        Map<String, Byte> topicAndGourpPerm = plainAccessResource.getResourcePermMap();

        Iterator<Entry<String, Byte>> it = topicAndGourpPerm.entrySet().iterator();
        Byte perm;
        while (it.hasNext()) {
            Entry<String, Byte> e = it.next();
            if ((perm = needCheckTopicAndGourpPerm.get(e.getKey())) != null && Permission.checkPermission(perm, e.getValue())) {
                continue;
            }
            byte neededPerm = PlainAccessResource.isRetryTopic(e.getKey()) ? needCheckplainAccessResource.getDefaultGroupPerm() :
                needCheckplainAccessResource.getDefaultTopicPerm();
            if (!Permission.checkPermission(neededPerm, e.getValue())) {
                throw new AclException(String.format("", e.toString()));
            }
        }
    }

    void cleanAuthenticationInfo() {
        this.plainAccessResourceMap.clear();
        this.globalWhiteRemoteAddressStrategy.clear();
    }

    public void setPlainAccessResource(PlainAccessResource plainAccessResource) throws AclException {
        if (plainAccessResource.getAccessKey() == null || plainAccessResource.getSecretKey() == null
            || plainAccessResource.getAccessKey().length() <= 6
            || plainAccessResource.getSecretKey().length() <= 6) {
            throw new AclException(String.format(
                "The account password cannot be null and is longer than 6, account is %s  password is %s",
                plainAccessResource.getAccessKey(), plainAccessResource.getSecretKey()));
        }
        try {
            RemoteAddressStrategy remoteAddressStrategy = remoteAddressStrategyFactory
                .getNetaddressStrategy(plainAccessResource);
            List<PlainAccessResource> accessControlAddressList = plainAccessResourceMap.get(plainAccessResource.getAccessKey());
            if (accessControlAddressList == null) {
                accessControlAddressList = new ArrayList<>();
                plainAccessResourceMap.put(plainAccessResource.getAccessKey(), accessControlAddressList);
            }
            plainAccessResource.setRemoteAddressStrategy(remoteAddressStrategy);

            accessControlAddressList.add(plainAccessResource);
            log.info("authenticationInfo is {}", plainAccessResource.toString());
        } catch (Exception e) {
            throw new AclException(
                String.format("Exception info %s  %s", e.getMessage(), plainAccessResource.toString()), e);
        }
    }

    private void setGlobalWhite(String remoteAddresses) {
        globalWhiteRemoteAddressStrategy.add(remoteAddressStrategyFactory.getNetaddressStrategy(remoteAddresses));
    }

    public void eachCheckPlainAccessResource(PlainAccessResource plainAccessResource) {

        List<PlainAccessResource> plainAccessResourceAddressList = plainAccessResourceMap.get(plainAccessResource.getAccessKey());
        boolean isDistinguishAccessKey = false;
        if (plainAccessResourceAddressList != null) {
            for (PlainAccessResource plainAccess : plainAccessResourceAddressList) {
                if (!plainAccess.getRemoteAddressStrategy().match(plainAccessResource)) {
                    isDistinguishAccessKey = true;
                    continue;
                }
                String signature = AclUtils.calSignature(plainAccessResource.getContent(), plainAccess.getSecretKey());
                if (signature.equals(plainAccessResource.getSignature())) {
                    checkPerm(plainAccess, plainAccessResource);
                    return;
                } else {
                    throw new AclException(String.format("signature is erron. erron accessKe is %s , erron reomiteAddress %s", plainAccess.getAccessKey(), plainAccessResource.getWhiteRemoteAddress()));
                }
            }
        }

        if (plainAccessResource.getAccessKey() == null && !globalWhiteRemoteAddressStrategy.isEmpty()) {
            for (RemoteAddressStrategy remoteAddressStrategy : globalWhiteRemoteAddressStrategy) {
                if (remoteAddressStrategy.match(plainAccessResource)) {
                    return;
                }
            }
        }
        if (isDistinguishAccessKey) {
            throw new AclException(String.format("client ip  not in WhiteRemoteAddress . erron accessKe is %s , erron reomiteAddress %s", plainAccessResource.getAccessKey(), plainAccessResource.getWhiteRemoteAddress()));
        } else {
            throw new AclException(String.format("It is not make Access and make client ip .erron accessKe is %s , erron reomiteAddress %s", plainAccessResource.getAccessKey(), plainAccessResource.getWhiteRemoteAddress()));
        }
    }

    public boolean isWatchStart() {
        return isWatchStart;
    }

}
