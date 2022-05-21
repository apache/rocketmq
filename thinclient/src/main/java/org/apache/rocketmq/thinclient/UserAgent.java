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

package org.apache.rocketmq.thinclient;

import apache.rocketmq.v2.Language;
import apache.rocketmq.v2.UA;
import org.apache.rocketmq.thinclient.misc.MetadataUtils;
import org.apache.rocketmq.thinclient.misc.Utilities;

public class UserAgent {
    private final String version;
    private final String platform;
    private final String hostName;

    private UserAgent(String version, String platform, String hostName) {
        this.version = version;
        this.platform = platform;
        this.hostName = hostName;
    }

    public static final UserAgent INSTANCE = new UserAgent(MetadataUtils.getVersion(), Utilities.getOsDescription(), Utilities.hostName());

    public UA toProtoBuf() {
        return UA.newBuilder()
            .setLanguage(Language.JAVA)
            .setVersion(version)
            .setPlatform(platform)
            .setHostname(hostName)
            .build();
    }
}
