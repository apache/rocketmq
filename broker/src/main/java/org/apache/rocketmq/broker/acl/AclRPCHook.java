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
package org.apache.rocketmq.broker.acl;

import static org.apache.rocketmq.common.protocol.RequestCode.PULL_MESSAGE;
import static org.apache.rocketmq.common.protocol.RequestCode.SEND_MESSAGE;
import org.apache.rocketmq.broker.out.BrokerOuterAPI;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.protocol.body.AclConfigData;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

/**
 * AclRpcHook used for access control list.
 */
public class AclRPCHook implements RPCHook {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);
    private BrokerOuterAPI brokerOuterAPI;
    // todo use nameserver listener to avoid call multiple time

    public AclRPCHook(BrokerOuterAPI brokerOuterAPI) {
        this.brokerOuterAPI = brokerOuterAPI;
    }

    /**
     *
     * @param remoteAddr
     * @param request
     */
    @Override
    public void doBeforeRequest(String remoteAddr, RemotingCommand request) {
        String access = "";
        try {
            AclConfigData acl = brokerOuterAPI.getAclData(remoteAddr);
            access = acl.getOperation();
        } catch (Exception e) {
            log.error("get acl data Exception, {}", e);
        }
        // check write cmd
        if (request.getCode() == SEND_MESSAGE && !access.equals("w")) {
            throw new RuntimeException("Access control is not permitted.");
        }

        // check read cmd
        if (request.getCode() == PULL_MESSAGE && !access.equals("r")) {
            throw new RuntimeException("Access control is not permitted.");
        }
    }

    @Override
    public void doAfterResponse(String remoteAddr, RemotingCommand request, RemotingCommand response) {

    }
}
