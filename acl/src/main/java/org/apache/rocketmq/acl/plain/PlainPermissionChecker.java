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
import org.apache.rocketmq.acl.common.AclException;
import org.apache.rocketmq.acl.common.Permission;

public class PlainPermissionChecker implements PermissionChecker {
    public void check(AccessResource checkedAccess, AccessResource ownedAccess) {
        PlainAccessResource checkedPlainAccess = (PlainAccessResource) checkedAccess;
        PlainAccessResource ownedPlainAccess = (PlainAccessResource) ownedAccess;

        if (ownedPlainAccess.isAdmin()) {
            // admin user don't need verification
            return;
        }
        if (Permission.needAdminPerm(checkedPlainAccess.getRequestCode())) {
            throw new AclException(String.format("Need admin permission for request code=%d, but accessKey=%s is not", checkedPlainAccess.getRequestCode(), ownedPlainAccess.getAccessKey()));
        }

        Map<String, Byte> needCheckedPermMap = checkedPlainAccess.getResourcePermMap();
        Map<String, Byte> ownedPermMap = ownedPlainAccess.getResourcePermMap();

        if (needCheckedPermMap == null) {
            // If the needCheckedPermMap is null,then return
            return;
        }

        for (Map.Entry<String, Byte> needCheckedEntry : needCheckedPermMap.entrySet()) {
            String resource = needCheckedEntry.getKey();
            Byte neededPerm = needCheckedEntry.getValue();
            boolean isGroup = PlainAccessResource.isRetryTopic(resource);

            if (ownedPermMap == null || !ownedPermMap.containsKey(resource)) {
                // Check the default perm
                byte ownedPerm = isGroup ? ownedPlainAccess.getDefaultGroupPerm() :
                    ownedPlainAccess.getDefaultTopicPerm();
                if (!Permission.checkPermission(neededPerm, ownedPerm)) {
                    throw new AclException(String.format("No default permission for %s", PlainAccessResource.printStr(resource, isGroup)));
                }
                continue;
            }
            if (!Permission.checkPermission(neededPerm, ownedPermMap.get(resource))) {
                throw new AclException(String.format("No permission for %s", PlainAccessResource.printStr(resource, isGroup)));
            }
        }
    }
}
