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

package org.apache.rocketmq.logging.dynamic;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * service factory
 */
public class ProcessUnitFactory {
    
    private static final Map<String, ProcessUnitFactory> pufMap = new ConcurrentHashMap<String, ProcessUnitFactory>();
    
    /**
     * service serverId
     */
    private String serverId = null;
    
    /**
     * Construct service factory
     *
     * @param serverId -- 服务serverId
     */
    public ProcessUnitFactory(String serverId) {
        this.serverId = serverId;
    }
    
    /**
     * 获取服务工厂
     *
     * @param serverId -- 服务serverId
     * @return -- 服务工厂
     */
    public static ProcessUnitFactory newInstance(String serverId) {
        ProcessUnitFactory puf = pufMap.get(serverId);
        if (puf == null) {
            puf = new ProcessUnitFactory(serverId);
            pufMap.put(serverId, puf);
        }
        return puf;
    }
    
    /**
     * Dynamic adjustment of log level
     */
    public AbstractProcessUnitImpl getChangeLogLevelProcess() {
        return ChangeLogLevelManager.getChageLogLevelProcess(serverId);
    }
    
    /**
     * method call processing unit
     */
    public AbstractProcessUnitImpl getMethodInvokerProcess() {
        return MethodInvokerManager.getMethodInvokerProcess(serverId);
    }
    
}
