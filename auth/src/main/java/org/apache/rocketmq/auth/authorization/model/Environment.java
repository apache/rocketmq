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
package org.apache.rocketmq.auth.authorization.model;

import java.util.Collections;
import java.util.List;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.utils.IPAddressUtils;

public class Environment {

    private List<String> sourceIps;

    public static Environment of(String sourceIp) {
        if (StringUtils.isEmpty(sourceIp)) {
            return null;
        }
        return of(Collections.singletonList(sourceIp));
    }

    public static Environment of(List<String> sourceIps) {
        if (CollectionUtils.isEmpty(sourceIps)) {
            return null;
        }
        Environment environment = new Environment();
        environment.setSourceIps(sourceIps);
        return environment;
    }

    public boolean isMatch(Environment environment) {
        if (CollectionUtils.isEmpty(this.sourceIps)) {
            return true;
        }
        if (CollectionUtils.isEmpty(environment.getSourceIps())) {
            return false;
        }
        String targetIp = environment.getSourceIps().get(0);
        for (String sourceIp : this.sourceIps) {
            if (IPAddressUtils.isIPInRange(targetIp, sourceIp)) {
                return true;
            }
        }
        return false;
    }

    public List<String> getSourceIps() {
        return sourceIps;
    }

    public void setSourceIps(List<String> sourceIps) {
        this.sourceIps = sourceIps;
    }
}
