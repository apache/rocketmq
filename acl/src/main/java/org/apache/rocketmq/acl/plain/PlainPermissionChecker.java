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
import org.apache.rocketmq.acl.common.AclException;
import org.apache.rocketmq.acl.common.Permission;

public class PlainPermissionChecker {
    public static void check(PlainAccessResource needCheckedAccess, PlainAccessResource ownedAccess) {
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
}
