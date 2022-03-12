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

package org.apache.rocketmq.example.benchmark;

import org.apache.rocketmq.acl.common.AclClientRPCHook;
import org.apache.rocketmq.acl.common.SessionCredentials;
import org.apache.rocketmq.remoting.RPCHook;

public class AclClient {

    public static final String ACL_ACCESS_KEY = "rocketmq2";

    public static final String ACL_SECRET_KEY = "12345678";

    public static RPCHook getAclRPCHook() {
        return getAclRPCHook(ACL_ACCESS_KEY, ACL_SECRET_KEY);
    }

    public static RPCHook getAclRPCHook(String ak, String sk) {
        return new AclClientRPCHook(new SessionCredentials(ak, sk));
    }
}
