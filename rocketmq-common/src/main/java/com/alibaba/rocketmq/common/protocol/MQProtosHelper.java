/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.alibaba.rocketmq.common.protocol;

import com.alibaba.rocketmq.common.protocol.header.namesrv.RegisterBrokerRequestHeader;
import com.alibaba.rocketmq.remoting.common.RemotingHelper;
import com.alibaba.rocketmq.remoting.exception.RemotingConnectException;
import com.alibaba.rocketmq.remoting.exception.RemotingSendRequestException;
import com.alibaba.rocketmq.remoting.exception.RemotingTimeoutException;
import com.alibaba.rocketmq.remoting.protocol.RemotingCommand;


/**
 * @author shijia.wxr
 */
public class MQProtosHelper {
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
        } catch (RemotingConnectException e) {
            e.printStackTrace();
        } catch (RemotingSendRequestException e) {
            e.printStackTrace();
        } catch (RemotingTimeoutException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        return false;
    }
}
