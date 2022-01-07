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
package org.apache.rocketmq.tools.command;

import org.apache.rocketmq.acl.common.AclClientRPCHook;
import org.apache.rocketmq.acl.common.SessionCredentials;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.junit.Before;
import org.powermock.api.mockito.PowerMockito;

public class SubCommandTest {

    public DefaultMQAdminExt defaultMQAdminExt;

    public RPCHook rpcHook;

    @Before
    public void init() throws Exception {
        rpcHook = new AclClientRPCHook(new SessionCredentials());
        defaultMQAdminExt = PowerMockito.mock(DefaultMQAdminExt.class);
        PowerMockito.whenNew(DefaultMQAdminExt.class).withArguments(rpcHook).thenReturn(defaultMQAdminExt);
    }
}
