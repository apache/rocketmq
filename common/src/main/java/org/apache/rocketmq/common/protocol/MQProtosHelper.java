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

package org.apache.rocketmq.common.protocol;

import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.protocol.header.namesrv.RegisterBrokerRequestHeader;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MQProtosHelper {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.COMMON_LOGGER_NAME);

    public static boolean registerBrokerToNameServer(final String nsaddr, final String brokerAddr,
        final long timeoutMillis) {
        RegisterBrokerRequestHeader requestHeader = new RegisterBrokerRequestHeader();
        requestHeader.setBrokerAddr(brokerAddr);

        RemotingCommand request =
            RemotingCommand.createRequestCommand(RequestCode.REGISTER_BROKER, requestHeader);

        try {
            RemotingCommand response = RemotingHelper.invokeSync(nsaddr, request, timeoutMillis);
            if (response != null) {
                return ResponseCode.SUCCESS == response.getCode();
            }
        } catch (Exception e) {
            log.error("Failed to register broker", e);
        }

        return false;
    }
}
