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
package org.apache.rocketmq.client.route.hook;

import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.protocol.RequestCode;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

public class AddZoneRPCHook implements RPCHook {

    public static final AddZoneRPCHook INSTANCE = new AddZoneRPCHook();
    /**
     * The name of zone to which producer or consumer belong.
     */
    private String zoneName = System.getProperty(MixAll.ROCKETMQ_CLIENT_ZONE_PROPERTY, System.getenv(MixAll.ROCKETMQ_CLIENT_ZONE_ENV));
    /**
     * Whether to enable obtaining routing info of the topic by zone.
     * The prerequisite is that The namesrv needs to enable nearby routing.
     * Producer or consumer can turn off nearby routing strategy when namesrv enable it.
     */
    private boolean zoneMode = Boolean.valueOf(System.getProperty(MixAll.ROCKETMQ_CLIENT_ZONE_MODE_PROPERTY, System.getenv(MixAll.ROCKETMQ_CLIENT_ZONE_MODE_ENV)));

    private AddZoneRPCHook() {
        
    }

    @Override
    public void doBeforeRequest(String remoteAddr, RemotingCommand request) {
        if (RequestCode.GET_ROUTEINFO_BY_TOPIC != request.getCode()) {
            return ;
        }
        if (StringUtils.isBlank(zoneName)) {
            return ;
        }
        request.addExtField(MixAll.ZONE_NAME, zoneName);
        request.addExtField(MixAll.ZONE_MODE, Boolean.toString(zoneMode));
    }

    @Override
    public void doAfterResponse(String remoteAddr, RemotingCommand request, RemotingCommand response) {
        
    }

    public String getZoneName() {
        return zoneName;
    }

    public void setZoneName(String zoneName) {
        this.zoneName = zoneName;
    }

    public boolean isZoneMode() {
        return zoneMode;
    }

    public void setZoneMode(boolean zoneMode) {
        this.zoneMode = zoneMode;
    }
}
