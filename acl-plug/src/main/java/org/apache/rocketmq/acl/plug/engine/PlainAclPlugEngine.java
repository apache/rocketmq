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
package org.apache.rocketmq.acl.plug.engine;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;

import org.apache.rocketmq.acl.plug.entity.AccessControl;
import org.apache.rocketmq.acl.plug.entity.BorkerAccessControlTransport;
import org.apache.rocketmq.acl.plug.entity.ControllerParametersEntity;
import org.apache.rocketmq.acl.plug.exception.AclPlugAccountAnalysisException;
import org.yaml.snakeyaml.Yaml;

public class PlainAclPlugEngine extends LoginInfoAclPlugEngine {

    private ControllerParametersEntity controllerParametersEntity;

    public PlainAclPlugEngine(
        ControllerParametersEntity controllerParametersEntity) throws AclPlugAccountAnalysisException {
        this.controllerParametersEntity = controllerParametersEntity;
        init();
    }

    void init() throws AclPlugAccountAnalysisException {
        String filePath = controllerParametersEntity.getFileHome() + "/conf/transport.yml";
        Yaml ymal = new Yaml();
        FileInputStream fis;
        try {
            fis = new FileInputStream(new File(filePath));
            BorkerAccessControlTransport transport = ymal.loadAs(fis, BorkerAccessControlTransport.class);
            super.setNetaddressAccessControl(transport.getOnlyNetAddress());
            for (AccessControl accessControl : transport.getList()) {
                super.setAccessControl(accessControl);
            }
        } catch (FileNotFoundException e) {
            throw new AclPlugAccountAnalysisException("The transport.yml file for Plain mode was not found", e);
        }
    }

}
