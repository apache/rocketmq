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

import org.apache.rocketmq.acl.plug.engine.AclPlugEngine;
import org.apache.rocketmq.acl.plug.engine.PlainAclPlugEngine;
import org.apache.rocketmq.acl.plug.entity.ControllerParametersEntity;
import org.apache.rocketmq.acl.plug.exception.AclPlugRuntimeException;

public class AclPlugController {

    private ControllerParametersEntity controllerParametersEntity;

    private AclPlugEngine aclPlugEngine;

    private AclRemotingServer aclRemotingServer;

    private boolean startSucceed = false;

    public AclPlugController(ControllerParametersEntity controllerParametersEntity) throws AclPlugRuntimeException {
        try {
            this.controllerParametersEntity = controllerParametersEntity;
            aclPlugEngine = new PlainAclPlugEngine(controllerParametersEntity);
            aclRemotingServer = new DefaultAclRemotingServerImpl(aclPlugEngine);
            this.startSucceed = true;
        } catch (Exception e) {
            throw new AclPlugRuntimeException(String.format("Start the abnormal , Launch parameters is %s", this.controllerParametersEntity.toString()), e);
        }
    }

    public AclRemotingServer getAclRemotingServer() {
        return this.aclRemotingServer;
    }

    public void doChannelCloseEvent(String remoteAddr) {
        if (this.startSucceed) {
            aclPlugEngine.deleteLoginInfo(remoteAddr);
        }
    }

    public boolean isStartSucceed() {
        return startSucceed;
    }
}
