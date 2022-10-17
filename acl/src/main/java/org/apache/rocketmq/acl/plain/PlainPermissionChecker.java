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

import java.util.Map;
import org.apache.rocketmq.acl.AccessResource;
import org.apache.rocketmq.acl.PermissionChecker;
import org.apache.rocketmq.acl.common.AclConstants;
import org.apache.rocketmq.acl.common.AclException;
import org.apache.rocketmq.acl.common.Permission;
import org.apache.rocketmq.common.protocol.NamespaceUtil;

public class PlainPermissionChecker implements PermissionChecker {
    public void check(AccessResource checkedAccess, AccessResource ownedAccess) {
        PlainAccessResource checkedPlainAccess = (PlainAccessResource) checkedAccess;
        PlainAccessResource ownedPlainAccess = (PlainAccessResource) ownedAccess;
        if (Permission.needAdminPerm(checkedPlainAccess.getRequestCode()) && !ownedPlainAccess.isAdmin()) {
            throw new AclException(String.format("Need admin permission for request code=%d, but accessKey=%s is not", checkedPlainAccess.getRequestCode(), ownedPlainAccess.getAccessKey()));
        }
        Map<String, Byte> needCheckedResourcePermMap = checkedPlainAccess.getResourcePermMap();
        Map<String, Byte> ownedResourcePermMap = ownedPlainAccess.getResourcePermMap();
        Map<String, Map<String, Byte>> ownedNamespacePermMap = ownedPlainAccess.getNamespacePermMap();

        if (needCheckedResourcePermMap == null) {
            // If the needCheckedPermMap is null,then return
            return;
        }

        if (ownedResourcePermMap == null && ownedPlainAccess.isAdmin()) {
            // If the ownedPermMap is null and it is an admin user, then return
            return;
        }

        for (Map.Entry<String, Byte> needCheckedEntry : needCheckedResourcePermMap.entrySet()) {
            String resource = needCheckedEntry.getKey();
            Byte neededPerm = needCheckedEntry.getValue();
            boolean isGroup = PlainAccessResource.isRetryTopic(resource);
            boolean isResourceContainsNamespace = NamespaceUtil.isContainNamespace(resource);

            //the resource perm that ak owned is null or doesn't contain the resource
            if (ownedResourcePermMap == null || !ownedResourcePermMap.containsKey(resource)) {
                //check the namespace perm and the default perm
                if (isMatchNamespaceOrDeafultPerm(isResourceContainsNamespace, resource, ownedNamespacePermMap, isGroup, neededPerm, ownedPlainAccess)) {
                    continue;
                } else {
                    throw new AclException(String.format("No default permission for %s", PlainAccessResource.printStr(resource, isGroup)));
                }
            } else {
                //check whether the resource perm that the ak owned is match the needed
                if (!Permission.checkPermission(neededPerm, ownedResourcePermMap.get(resource))) {
                    throw new AclException(String.format("No default permission for %s", PlainAccessResource.printStr(resource, isGroup)));
                }
                continue;
            }
        }
    }

    public boolean isMatchNamespaceOrDeafultPerm(boolean isResourceContainsNamespace, String resource, Map<String, Map<String, Byte>> ownedNamespacePermMap,
        boolean isGroup, byte neededPerm, PlainAccessResource ownedPlainAccess) {
        if (isResourceContainsNamespace) {
            String namespace = PlainAccessResource.getResourceNamespace(resource);
            if (ownedNamespacePermMap.containsKey(namespace)) {
                Map<String, Byte> defaultPermMap = ownedNamespacePermMap.get(namespace);
                byte ownedPerm = isGroup ? defaultPermMap.get(AclConstants.CONFIG_GROUP_PERM) : defaultPermMap.get(AclConstants.CONFIG_TOPIC_PERM);
                if (!Permission.checkPermission(neededPerm, ownedPerm)) {
                    //check the defaultTopicPerm or defaultGroupPerm according to the type of the resource
                    ownedPerm = isGroup ? ownedPlainAccess.getDefaultGroupPerm() : ownedPlainAccess.getDefaultTopicPerm();
                    if (!Permission.checkPermission(neededPerm, ownedPerm)) {
                        return false;
                    }
                    return true;
                }
                return true;
            } else {
                //the ownedNamespacePermMap doesn't have the namespace
                //check the defaultTopicPerm or defaultGroupPerm according to the type of the resource
                byte ownedPerm = isGroup ? ownedPlainAccess.getDefaultGroupPerm() : ownedPlainAccess.getDefaultTopicPerm();
                if (!Permission.checkPermission(neededPerm, ownedPerm)) {
                    return false;
                }
                return true;
            }
        } else {
            //check the defaultTopicPerm or defaultGroupPerm according to the type of the resource
            byte ownedPerm = isGroup ? ownedPlainAccess.getDefaultGroupPerm() : ownedPlainAccess.getDefaultTopicPerm();
            if (!Permission.checkPermission(neededPerm, ownedPerm)) {
                return false;
            }
            return true;
        }
    }

}
