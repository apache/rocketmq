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
package org.apache.rocketmq.acl.plug.strategy;

import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.acl.plug.AclUtils;
import org.apache.rocketmq.acl.plug.entity.AccessControl;

public class NetaddressStrategyFactory {

    public NetaddressStrategy getNetaddressStrategy(AccessControl accessControl) {
        String netaddress = accessControl.getNetaddress();
        if (StringUtils.isBlank(netaddress) || "*".equals(netaddress)) {
            return NullNetaddressStrategy.NULL_NET_ADDRESS_STRATEGY;
        }
        if (netaddress.endsWith("}")) {
            String[] strArray = StringUtils.split(netaddress);
            String four = strArray[3];
            if (!four.startsWith("{")) {

            }
            return new MultipleNetaddressStrategy(AclUtils.getAddreeStrArray(netaddress, four));
        } else if (AclUtils.isColon(netaddress)) {
            return new MultipleNetaddressStrategy(StringUtils.split(","));
        } else if (AclUtils.isAsterisk(netaddress) || AclUtils.isMinus(netaddress)) {
            return new RangeNetaddressStrategy(netaddress);
        }
        return new OneNetaddressStrategy(netaddress);

    }
}
