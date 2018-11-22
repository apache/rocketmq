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
package org.apache.rocketmq.acl.plug;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.protocol.RequestCode;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;

public class PlainAclPlugEngine {

    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.ACL_PLUG_LOGGER_NAME);

    private String fileHome = System.getProperty(MixAll.ROCKETMQ_HOME_PROPERTY,
        System.getenv(MixAll.ROCKETMQ_HOME_ENV));

    private Map<String/** account **/, List<AuthenticationInfo>> accessControlMap = new HashMap<>();

    private AuthenticationInfo authenticationInfo;

    private NetaddressStrategyFactory netaddressStrategyFactory = new NetaddressStrategyFactory();

    private AccessContralAnalysis accessContralAnalysis = new AccessContralAnalysis();

    private Class<?> accessContralAnalysisClass = RequestCode.class;

    public PlainAclPlugEngine() {
        initialize();
    }

    public void initialize() {
        BorkerAccessControlTransport accessControlTransport = AclUtils.getYamlDataObject(fileHome + "/conf/transport.yml", BorkerAccessControlTransport.class);
        if (accessControlTransport == null) {
            throw new AclPlugRuntimeException("transport.yml file  is no data");
        }
        log.info("BorkerAccessControlTransport data is : ", accessControlTransport.toString());
        accessContralAnalysis.analysisClass(accessContralAnalysisClass);
        setBorkerAccessControlTransport(accessControlTransport);
    }

    public void setAccessControl(AccessControl accessControl) throws AclPlugRuntimeException {
        if (accessControl.getAccount() == null || accessControl.getPassword() == null
            || accessControl.getAccount().length() <= 6 || accessControl.getPassword().length() <= 6) {
            throw new AclPlugRuntimeException(String.format(
                "The account password cannot be null and is longer than 6, account is %s  password is %s",
                accessControl.getAccount(), accessControl.getPassword()));
        }
        try {
            NetaddressStrategy netaddressStrategy = netaddressStrategyFactory.getNetaddressStrategy(accessControl);
            List<AuthenticationInfo> accessControlAddressList = accessControlMap.get(accessControl.getAccount());
            if (accessControlAddressList == null) {
                accessControlAddressList = new ArrayList<>();
                accessControlMap.put(accessControl.getAccount(), accessControlAddressList);
            }
            AuthenticationInfo authenticationInfo = new AuthenticationInfo(
                accessContralAnalysis.analysis(accessControl), accessControl, netaddressStrategy);
            accessControlAddressList.add(authenticationInfo);
            log.info("authenticationInfo is {}", authenticationInfo.toString());
        } catch (Exception e) {
            throw new AclPlugRuntimeException(
                String.format("Exception info %s  %s", e.getMessage(), accessControl.toString()), e);
        }
    }

    public void setAccessControlList(List<AccessControl> accessControlList) throws AclPlugRuntimeException {
        for (AccessControl accessControl : accessControlList) {
            setAccessControl(accessControl);
        }
    }

    public void setNetaddressAccessControl(AccessControl accessControl) throws AclPlugRuntimeException {
        try {
            authenticationInfo = new AuthenticationInfo(accessContralAnalysis.analysis(accessControl), accessControl, netaddressStrategyFactory.getNetaddressStrategy(accessControl));
            log.info("default authenticationInfo is {}", authenticationInfo.toString());
        } catch (Exception e) {
            throw new AclPlugRuntimeException(accessControl.toString(), e);
        }

    }

    public AuthenticationInfo getAccessControl(AccessControl accessControl) {
        if (accessControl.getAccount() == null && authenticationInfo != null) {
            return authenticationInfo.getNetaddressStrategy().match(accessControl) ? authenticationInfo : null;
        } else {
            List<AuthenticationInfo> accessControlAddressList = accessControlMap.get(accessControl.getAccount());
            if (accessControlAddressList != null) {
                for (AuthenticationInfo ai : accessControlAddressList) {
                    if (ai.getNetaddressStrategy().match(accessControl) && ai.getAccessControl().getPassword().equals(accessControl.getPassword())) {
                        return ai;
                    }
                }
            }
        }
        return null;
    }

    public AuthenticationResult eachCheckAuthentication(AccessControl accessControl) {
        AuthenticationResult authenticationResult = new AuthenticationResult();
        AuthenticationInfo authenticationInfo = getAccessControl(accessControl);
        if (authenticationInfo != null) {
            boolean boo = authentication(authenticationInfo, accessControl, authenticationResult);
            authenticationResult.setSucceed(boo);
            authenticationResult.setAccessControl(authenticationInfo.getAccessControl());
        } else {
            authenticationResult.setResultString("accessControl is null, Please check login, password, IP\"");
        }
        return authenticationResult;
    }

    void setBorkerAccessControlTransport(BorkerAccessControlTransport transport) {
        if (transport.getOnlyNetAddress() == null && (transport.getList() == null || transport.getList().size() == 0)) {
            throw new AclPlugRuntimeException("onlyNetAddress and list  can't be all empty");
        }

        if (transport.getOnlyNetAddress() != null) {
            this.setNetaddressAccessControl(transport.getOnlyNetAddress());
        }
        if (transport.getList() != null || transport.getList().size() > 0) {
            for (AccessControl accessControl : transport.getList()) {
                this.setAccessControl(accessControl);
            }
        }
    }

    public boolean authentication(AuthenticationInfo authenticationInfo, AccessControl accessControl,
        AuthenticationResult authenticationResult) {
        int code = accessControl.getCode();
        if (!authenticationInfo.getAuthority().get(code)) {
            authenticationResult.setResultString(String.format("code is %d Authentication failed", code));
            return false;
        }
        if (!(authenticationInfo.getAccessControl() instanceof BorkerAccessControl)) {
            return true;
        }
        BorkerAccessControl borker = (BorkerAccessControl) authenticationInfo.getAccessControl();
        String topicName = accessControl.getTopic();
        if (code == 10 || code == 310 || code == 320) {
            if (borker.getPermitSendTopic().contains(topicName)) {
                return true;
            }
            if (borker.getNoPermitSendTopic().contains(topicName)) {
                authenticationResult.setResultString(String.format("noPermitSendTopic include %s", topicName));
                return false;
            }
            return borker.getPermitSendTopic().isEmpty() ? true : false;
        } else if (code == 11) {
            if (borker.getPermitPullTopic().contains(topicName)) {
                return true;
            }
            if (borker.getNoPermitPullTopic().contains(topicName)) {
                authenticationResult.setResultString(String.format("noPermitPullTopic include %s", topicName));
                return false;
            }
            return borker.getPermitPullTopic().isEmpty() ? true : false;
        }
        return true;
    }

    public static class AccessContralAnalysis {

        private Map<Class<?>, Map<Integer, Field>> classTocodeAndMentod = new HashMap<>();

        private Map<String, Integer> fieldNameAndCode = new HashMap<>();

        public void analysisClass(Class<?> clazz) {
            Field[] fields = clazz.getDeclaredFields();
            try {
                for (Field field : fields) {
                    if (field.getType().equals(int.class)) {
                        String name = StringUtils.replace(field.getName(), "_", "").toLowerCase();
                        fieldNameAndCode.put(name, (Integer) field.get(null));
                    }
                }
            } catch (IllegalArgumentException | IllegalAccessException e) {
                throw new AclPlugRuntimeException(String.format("analysis on failure Class is %s", clazz.getName()), e);
            }
        }

        public Map<Integer, Boolean> analysis(AccessControl accessControl) {
            Class<? extends AccessControl> clazz = accessControl.getClass();
            Map<Integer, Field> codeAndField = classTocodeAndMentod.get(clazz);
            if (codeAndField == null) {
                codeAndField = new HashMap<>();
                Field[] fields = clazz.getDeclaredFields();
                for (Field field : fields) {
                    if (!field.getType().equals(boolean.class))
                        continue;
                    Integer code = fieldNameAndCode.get(field.getName().toLowerCase());
                    if (code == null) {
                        throw new AclPlugRuntimeException(
                            String.format("field nonexistent in code  fieldName is %s", field.getName()));
                    }
                    field.setAccessible(true);
                    codeAndField.put(code, field);

                }
                if (codeAndField.isEmpty()) {
                    throw new AclPlugRuntimeException(String.format("AccessControl nonexistent code , name %s",
                        accessControl.getClass().getName()));
                }
                classTocodeAndMentod.put(clazz, codeAndField);
            }
            Iterator<Entry<Integer, Field>> it = codeAndField.entrySet().iterator();
            Map<Integer, Boolean> authority = new HashMap<>();
            try {
                while (it.hasNext()) {
                    Entry<Integer, Field> e = it.next();
                    authority.put(e.getKey(), (Boolean) e.getValue().get(accessControl));
                }
            } catch (IllegalArgumentException | IllegalAccessException e) {
                throw new AclPlugRuntimeException(
                    String.format("analysis on failure AccessControl is %s", AccessControl.class.getName()), e);
            }
            return authority;
        }

    }

    public static class BorkerAccessControlTransport {

        private BorkerAccessControl onlyNetAddress;

        private List<BorkerAccessControl> list;

        public BorkerAccessControl getOnlyNetAddress() {
            return onlyNetAddress;
        }

        public void setOnlyNetAddress(BorkerAccessControl onlyNetAddress) {
            this.onlyNetAddress = onlyNetAddress;
        }

        public List<BorkerAccessControl> getList() {
            return list;
        }

        public void setList(List<BorkerAccessControl> list) {
            this.list = list;
        }

        @Override
        public String toString() {
            return "BorkerAccessControlTransport [onlyNetAddress=" + onlyNetAddress + ", list=" + list + "]";
        }
    }
}
