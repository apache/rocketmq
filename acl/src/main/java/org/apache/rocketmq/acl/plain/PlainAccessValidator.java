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

import com.google.protobuf.GeneratedMessageV3;
import java.util.List;
import java.util.Map;
import org.apache.rocketmq.acl.AccessResource;
import org.apache.rocketmq.acl.AccessValidator;
import org.apache.rocketmq.acl.common.AuthenticationHeader;
import org.apache.rocketmq.common.AclConfig;
import org.apache.rocketmq.common.PlainAccessConfig;
import org.apache.rocketmq.remoting.protocol.DataVersion;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

public class PlainAccessValidator implements AccessValidator {

    private PlainPermissionManager aclPlugEngine;

    public PlainAccessValidator() {
        aclPlugEngine = new PlainPermissionManager();
    }

    @Override
    public AccessResource parse(RemotingCommand request, String remoteAddr) {
        return PlainAccessResource.parse(request, remoteAddr);
    }

    @Override
    public AccessResource parse(GeneratedMessageV3 messageV3, AuthenticationHeader header) {
        return PlainAccessResource.parse(messageV3, header);
    }

    @Override
    public void validate(AccessResource accessResource) {
        aclPlugEngine.validate((PlainAccessResource) accessResource);
    }

    @Override
    public boolean updateAccessConfig(PlainAccessConfig plainAccessConfig) {
        return aclPlugEngine.updateAccessConfig(plainAccessConfig);
    }

    @Override
    public boolean deleteAccessConfig(String accessKey) {
        return aclPlugEngine.deleteAccessConfig(accessKey);
    }

    @Override
    public String getAclConfigVersion() {
        return aclPlugEngine.getAclConfigDataVersion();
        // 替换为新的方法名
        //return aclPlugEngine.getCurrentAclConfigVersion();
    }

    @Override
    public boolean updateGlobalWhiteAddrsConfig(List<String> globalWhiteAddrsList) {
        return aclPlugEngine.updateGlobalWhiteAddrsConfig(globalWhiteAddrsList);
    }

    @Override
    public boolean updateGlobalWhiteAddrsConfig(List<String> globalWhiteAddrsList, String aclFileFullPath) {
        return aclPlugEngine.updateGlobalWhiteAddrsConfig(globalWhiteAddrsList, aclFileFullPath);
    }

    @Override
    public AclConfig getAllAclConfig() {
        return aclPlugEngine.getAllAclConfig();
    }

    @Override
    public Map<String, DataVersion> getAllAclConfigVersion() {
        return aclPlugEngine.getDataVersionMap();
    }
}
