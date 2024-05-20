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
package org.apache.rocketmq.ratelimit.config;

import org.apache.rocketmq.ratelimit.provider.LocalRatelimitMetadataProvider;

public class RatelimitConfig implements Cloneable {

    private String ratelimitConfigPath;

    private boolean ratelimitEnabled = false;

    private String ratelimitProvider;

    private String ratelimitMetadataProvider = LocalRatelimitMetadataProvider.class.getName();

    private int ratelimiterCacheMaxSize = 10000;

    private int ratelimitRuleMaxNum = 20;


    public String getRatelimitConfigPath() {
        return ratelimitConfigPath;
    }

    public void setRatelimitConfigPath(String ratelimitConfigPath) {
        this.ratelimitConfigPath = ratelimitConfigPath;
    }

    public boolean isRatelimitEnabled() {
        return ratelimitEnabled;
    }

    public void setRatelimitEnabled(boolean ratelimitEnabled) {
        this.ratelimitEnabled = ratelimitEnabled;
    }

    public String getRatelimitProvider() {
        return ratelimitProvider;
    }

    public void setRatelimitProvider(String ratelimitProvider) {
        this.ratelimitProvider = ratelimitProvider;
    }

    public String getRatelimitMetadataProvider() {
        return ratelimitMetadataProvider;
    }

    public void setRatelimitMetadataProvider(String ratelimitMetadataProvider) {
        this.ratelimitMetadataProvider = ratelimitMetadataProvider;
    }

    public int getRatelimiterCacheMaxSize() {
        return ratelimiterCacheMaxSize;
    }

    public void setRatelimiterCacheMaxSize(int ratelimiterCacheMaxSize) {
        this.ratelimiterCacheMaxSize = ratelimiterCacheMaxSize;
    }

    public int getRatelimitRuleMaxNum() {
        return ratelimitRuleMaxNum;
    }

    public void setRatelimitRuleMaxNum(int ratelimitRuleMaxNum) {
        this.ratelimitRuleMaxNum = ratelimitRuleMaxNum;
    }
}
