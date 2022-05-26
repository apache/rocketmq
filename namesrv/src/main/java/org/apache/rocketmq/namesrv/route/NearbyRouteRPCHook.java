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
package org.apache.rocketmq.namesrv.route;

import org.apache.rocketmq.common.protocol.RequestCode;
import org.apache.rocketmq.common.protocol.ResponseCode;
import org.apache.rocketmq.common.protocol.route.TopicRouteData;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;

public class NearbyRouteRPCHook implements RPCHook {

	@Override
	public void doBeforeRequest(String remoteAddr, RemotingCommand request) {

	}

	@Override
	public void doAfterResponse(String remoteAddr, RemotingCommand request, RemotingCommand response) {
		if(RequestCode.GET_ROUTEINFO_BY_TOPIC != request.getCode()) {
			return;
		}
		if(!NearbyRouteManager.INSTANCE.isNearbyRoute()) {
			return;
		}
		if(NearbyRouteManager.INSTANCE.isWhiteRemoteAddresses(remoteAddr)){
			return;
		}
		if (response == null || response.getBody() == null || ResponseCode.SUCCESS != response.getCode()) {
			return;
		}
		TopicRouteData topicRouteData = RemotingSerializable.decode(response.getBody(), TopicRouteData.class);
		
		response.setBody(NearbyRouteManager.INSTANCE.filter(remoteAddr, topicRouteData).encode());
	}

}
