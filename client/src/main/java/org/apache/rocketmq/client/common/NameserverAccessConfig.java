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

package org.apache.rocketmq.client.common;

public class NameserverAccessConfig {
    private String namesrvAddr;
    private String namesrvDomain;
    private String namesrvDomainSubgroup;

    public NameserverAccessConfig(String namesrvAddr, String namesrvDomain, String namesrvDomainSubgroup) {
        this.namesrvAddr = namesrvAddr;
        this.namesrvDomain = namesrvDomain;
        this.namesrvDomainSubgroup = namesrvDomainSubgroup;
    }

    public String getNamesrvAddr() {
        return namesrvAddr;
    }

    public String getNamesrvDomain() {
        return namesrvDomain;
    }

    public String getNamesrvDomainSubgroup() {
        return namesrvDomainSubgroup;
    }
}
