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
package org.apache.rocketmq.acl.plug.entity;

import org.apache.rocketmq.acl.plug.strategy.NetaddressStrategy;

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

public class AuthenticationInfo {

    private AccessControl accessControl;

    private NetaddressStrategy netaddressStrategy;

    private Map<Integer, Boolean> authority;

    public AuthenticationInfo(Map<Integer, Boolean> authority, AccessControl accessControl,
                              NetaddressStrategy netaddressStrategy) {
        super();
        this.authority = authority;
        this.accessControl = accessControl;
        this.netaddressStrategy = netaddressStrategy;
    }

    public AccessControl getAccessControl() {
        return accessControl;
    }

    public void setAccessControl(AccessControl accessControl) {
        this.accessControl = accessControl;
    }

    public NetaddressStrategy getNetaddressStrategy() {
        return netaddressStrategy;
    }

    public void setNetaddressStrategy(NetaddressStrategy netaddressStrategy) {
        this.netaddressStrategy = netaddressStrategy;
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
        builder.append("AuthenticationInfo [accessControl=").append(accessControl).append(", netaddressStrategy=")
                .append(netaddressStrategy).append(", authority={");
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
