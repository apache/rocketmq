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
package org.apache.rocketmq.remoting.rpc;

import java.util.concurrent.Future;
import org.apache.rocketmq.common.message.MessageQueue;

public interface RpcClient {


    //common invoke paradigm, the logic remote addr is defined in "bname" field of request
    //For oneway request, the sign is labeled in request, and do not need an another method named "invokeOneway"
    //For one
    Future<RpcResponse>  invoke(RpcRequest request, long timeoutMs) throws RpcException;

    //For rocketmq, most requests are corresponded to MessageQueue
    //And for LogicQueue, the broker name is mocked, the physical addr could only be defined by MessageQueue
    Future<RpcResponse>  invoke(MessageQueue mq, RpcRequest request, long timeoutMs) throws RpcException;

}
