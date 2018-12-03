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

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

@Deprecated
public class AuthenticationInfo {

    private PlainAccessResource plainAccessResource;

    private RemoteAddressStrategy remoteAddressStrategy;

    private Map<Integer, Boolean> authority;

    public AuthenticationInfo(Map<Integer, Boolean> authority, PlainAccessResource plainAccessResource,
        RemoteAddressStrategy remoteAddressStrategy) {
        super();
        this.authority = authority;
        this.plainAccessResource = plainAccessResource;
        this.remoteAddressStrategy = remoteAddressStrategy;
    }

    public PlainAccessResource getPlainAccessResource() {
        return plainAccessResource;
    }

    public void setPlainAccessResource(PlainAccessResource plainAccessResource) {
        this.plainAccessResource = plainAccessResource;
    }

    public RemoteAddressStrategy getRemoteAddressStrategy() {
        return remoteAddressStrategy;
    }

    public void setRemoteAddressStrategy(RemoteAddressStrategy remoteAddressStrategy) {
        this.remoteAddressStrategy = remoteAddressStrategy;
    }

    public Map<Integer, Boolean> getAuthority() {
        return authority;
    }

    public void setAuthority(Map<Integer, Boolean> authority) {
        this.authority = authority;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("AuthenticationInfo [plainAccessResource=").append(plainAccessResource).append(", remoteAddressStrategy=")
            .append(remoteAddressStrategy).append(", authority={");
        Iterator<Entry<Integer, Boolean>> it = authority.entrySet().iterator();
        while (it.hasNext()) {
            Entry<Integer, Boolean> e = it.next();
            if (!e.getValue()) {
                builder.append(e.getKey().toString()).append(":").append(e.getValue()).append(",");
            }
        }
        builder.append("}]");
        return builder.toString();
    }

}
