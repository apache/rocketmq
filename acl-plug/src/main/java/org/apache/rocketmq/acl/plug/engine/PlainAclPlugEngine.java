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
import java.io.IOException;
import org.apache.rocketmq.acl.plug.entity.BorkerAccessControlTransport;
import org.apache.rocketmq.acl.plug.entity.ControllerParameters;
import org.apache.rocketmq.acl.plug.exception.AclPlugRuntimeException;
import org.yaml.snakeyaml.Yaml;

public class PlainAclPlugEngine extends LoginInfoAclPlugEngine {

    public PlainAclPlugEngine(
        ControllerParameters controllerParameters) throws AclPlugRuntimeException {
        super(controllerParameters);
    }

    public void initialize() throws AclPlugRuntimeException {
        String filePath = controllerParameters.getFileHome() + "/conf/transport.yml";
        Yaml ymal = new Yaml();
        FileInputStream fis = null;
        BorkerAccessControlTransport transport;
        try {
            fis = new FileInputStream(new File(filePath));
            transport = ymal.loadAs(fis, BorkerAccessControlTransport.class);
        } catch (Exception e) {
            throw new AclPlugRuntimeException(String.format("The transport.yml file for Plain mode was not found , paths %s", filePath), e);
        } finally {
            if (fis != null) {
                try {
                    fis.close();
                } catch (IOException e) {
                    throw new AclPlugRuntimeException("close transport fileInputStream Exception", e);
                }
            }
        }
        if (transport == null) {
            throw new AclPlugRuntimeException("transport.yml file  is no data");
        }
        super.setBorkerAccessControlTransport(transport);
    }

}
