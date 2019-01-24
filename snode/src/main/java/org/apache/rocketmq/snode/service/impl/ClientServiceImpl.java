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
package org.apache.rocketmq.snode.service.impl;
import java.util.List;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.protocol.RequestCode;
import org.apache.rocketmq.common.protocol.header.NotifyConsumerIdsChangedRequestHeader;
import org.apache.rocketmq.common.subscription.SubscriptionGroupConfig;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.remoting.RemotingChannel;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.snode.SnodeController;
import org.apache.rocketmq.snode.service.ClientService;

public class ClientServiceImpl implements ClientService {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.SNODE_LOGGER_NAME);

    private final SnodeController snodeController;

    public ClientServiceImpl(SnodeController snodeController) {
        this.snodeController = snodeController;
    }

    @Override
    public void notifyConsumer(String group) {
        if (group != null) {
            SubscriptionGroupConfig subscriptionGroupConfig = snodeController.getSubscriptionGroupManager().findSubscriptionGroupConfig(group);
            boolean notifyConsumer = subscriptionGroupConfig.isNotifyConsumerIdsChangedEnable();
            if (notifyConsumer) {
                List<RemotingChannel> remotingChannels = snodeController.getConsumerManager().getChannels(group);
                if (remotingChannels != null && snodeController.getSubscriptionGroupManager().getSubscriptionGroupTable().get(group).isNotifyConsumerIdsChangedEnable()) {
                    for (RemotingChannel remotingChannel : remotingChannels) {
                        NotifyConsumerIdsChangedRequestHeader requestHeader = new NotifyConsumerIdsChangedRequestHeader();
                        requestHeader.setConsumerGroup(group);
                        RemotingCommand request =
                            RemotingCommand.createRequestCommand(RequestCode.NOTIFY_CONSUMER_IDS_CHANGED, requestHeader);
                        try {
                            this.snodeController.getSnodeServer().invokeOneway(remotingChannel, request, 10);
                        } catch (Exception e) {
                            log.error("NotifyConsumerIdsChanged exception, " + group, e.getMessage());
                        }
                    }
                }
            }
        }
    }

}
