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
package org.apache.rocketmq.remoting;

import java.util.Map;
import org.apache.rocketmq.remoting.common.RemotingUtil;
import org.apache.rocketmq.remoting.util.ServiceProvider;

public class RemotingServerFactory {
    private static RemotingServerFactory instance = new RemotingServerFactory();

    public static RemotingServerFactory getInstance() {
        return instance;
    }

    private RemotingServerFactory() {
    }

    private static Map<String, String> protocolPathMap;

    private static final String SERVER_LOCATION = "META-INF/service/org.apache.rocketmq.remoting.RemotingServer";

    static {
        protocolPathMap = ServiceProvider.loadPath(SERVER_LOCATION);
    }

    public RemotingServer createRemotingServer(String protocol) {
        return ServiceProvider.createInstance(protocolPathMap.get(protocol), RemotingServer.class);
    }

    public RemotingServer createRemotingServer() {
        return ServiceProvider.createInstance(protocolPathMap.get(RemotingUtil.DEFAULT_PROTOCOL), RemotingServer.class);
    }
}
