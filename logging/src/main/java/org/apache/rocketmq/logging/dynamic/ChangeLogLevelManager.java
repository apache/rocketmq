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
 * Service configuration
 */
public final class ChangeLogLevelManager {
    
    /**
     * Collection of log tuning services
     */
    private static final Map<String, ChangeLogLevelProcessUnit> cllpuMap = new ConcurrentHashMap<String, ChangeLogLevelProcessUnit>();
    
    /**
     * Get service instance based on serverId
     *
     * @param serverId -- service serverId
     * @return -- log adjustment service object
     */
    public static ChangeLogLevelProcessUnit getChageLogLevelProcess(String serverId) {
        ChangeLogLevelProcessUnit process = cllpuMap.get(serverId);
        if (null == process) {
            synchronized (cllpuMap) {
                if (null == process) {
                    process = new ChangeLogLevelProcessUnit();
                    cllpuMap.put(serverId, process);
                }
            }
        }
        return process;
    }
}
