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
package org.apache.rocketmq.snode.processor;

import java.util.concurrent.CompletableFuture;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.help.FAQUrl;
import org.apache.rocketmq.common.protocol.ResponseCode;
import org.apache.rocketmq.common.protocol.header.PullMessageRequestHeader;
import org.apache.rocketmq.common.protocol.header.PullMessageResponseHeader;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
import org.apache.rocketmq.common.protocol.heartbeat.SubscriptionData;
import org.apache.rocketmq.common.subscription.SubscriptionGroupConfig;
import org.apache.rocketmq.common.sysflag.PullSysFlag;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.remoting.RemotingChannel;
import org.apache.rocketmq.remoting.RequestProcessor;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.interceptor.ExceptionContext;
import org.apache.rocketmq.remoting.interceptor.RequestContext;
import org.apache.rocketmq.remoting.interceptor.ResponseContext;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.snode.SnodeController;
import org.apache.rocketmq.snode.client.impl.Subscription;

public class PullMessageProcessor implements RequestProcessor {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.SNODE_LOGGER_NAME);

    private final SnodeController snodeController;

    public PullMessageProcessor(SnodeController snodeController) {
        this.snodeController = snodeController;
    }

    @Override
    public RemotingCommand processRequest(RemotingChannel remotingChannel,
        RemotingCommand request) throws RemotingCommandException {
        if (this.snodeController.getConsumeMessageInterceptorGroup() != null) {
            RequestContext requestContext = new RequestContext(request, remotingChannel);
            this.snodeController.getConsumeMessageInterceptorGroup().beforeRequest(requestContext);
        }
        RemotingCommand response = pullMessage(remotingChannel, request);
        if (this.snodeController.getConsumeMessageInterceptorGroup() != null && response != null) {
            ResponseContext responseContext = new ResponseContext(request, remotingChannel, response);
            this.snodeController.getSendMessageInterceptorGroup().afterRequest(responseContext);
        }
        return response;
    }

    private RemotingCommand pullMessage(RemotingChannel remotingChannel,
        RemotingCommand request) throws RemotingCommandException {
        RemotingCommand response = RemotingCommand.createResponseCommand(PullMessageResponseHeader.class);
        response.setOpaque(request.getOpaque());
        final PullMessageRequestHeader requestHeader =
            (PullMessageRequestHeader) request.decodeCommandCustomHeader(PullMessageRequestHeader.class);
        SubscriptionGroupConfig subscriptionGroupConfig =
            this.snodeController.getSubscriptionGroupManager().findSubscriptionGroupConfig(requestHeader.getConsumerGroup());
        if (null == subscriptionGroupConfig) {
            response.setCode(ResponseCode.SUBSCRIPTION_GROUP_NOT_EXIST);
            response.setRemark(String.format("Subscription group [%s] does not exist, %s", requestHeader.getConsumerGroup(), FAQUrl.suggestTodo(FAQUrl.SUBSCRIPTION_GROUP_NOT_EXIST)));
            return response;
        }

        if (!subscriptionGroupConfig.isConsumeEnable()) {
            response.setCode(ResponseCode.NO_PERMISSION);
            response.setRemark("Subscription group no permission, " + requestHeader.getConsumerGroup());
            return response;
        }

        final boolean hasSubscriptionFlag = PullSysFlag.hasSubscriptionFlag(requestHeader.getSysFlag());

        if (requestHeader.getQueueId() < 0) {
            String errorInfo = String.format("QueueId[%d] is illegal, topic:[%s] consumer:[%s]",
                requestHeader.getQueueId(), requestHeader.getTopic(), remotingChannel.remoteAddress());
            log.warn(errorInfo);
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark(errorInfo);
            return response;
        }

        SubscriptionData subscriptionData;
        if (!hasSubscriptionFlag) {
            Subscription subscription = this.snodeController.getSubscriptionManager().getSubscription(requestHeader.getConsumerGroup());
            if (null == subscription) {
                log.warn("The consumer's group info not exist, group: {}", requestHeader.getConsumerGroup());
                response.setCode(ResponseCode.SUBSCRIPTION_NOT_EXIST);
                response.setRemark("The consumer's group info not exist" + FAQUrl.suggestTodo(FAQUrl.SAME_GROUP_DIFFERENT_TOPIC));
                return response;
            }

            if (!subscriptionGroupConfig.isConsumeBroadcastEnable()
                && subscription.getMessageModel() == MessageModel.BROADCASTING) {
                response.setCode(ResponseCode.NO_PERMISSION);
                response.setRemark("The consumer group[" + requestHeader.getConsumerGroup() + "] can not consume by broadcast way");
                return response;
            }

            subscriptionData = subscription.getSubscriptionData(requestHeader.getTopic());
            if (null == subscriptionData) {
                log.warn("The consumer's subscription not exist, group: {}, topic:{}", requestHeader.getConsumerGroup(), requestHeader.getTopic());
                response.setCode(ResponseCode.SUBSCRIPTION_NOT_EXIST);
                response.setRemark("The consumer's subscription not exist" + FAQUrl.suggestTodo(FAQUrl.SAME_GROUP_DIFFERENT_TOPIC));
                return response;
            }

            if (subscriptionData.getSubVersion() < requestHeader.getSubVersion()) {
                log.warn("The broker's subscription is not latest, group: {} {}", requestHeader.getConsumerGroup(),
                    subscriptionData.getSubString());
                response.setCode(ResponseCode.SUBSCRIPTION_NOT_LATEST);
                response.setRemark("The consumer's subscription not latest");
                return response;
            }
        }

        CompletableFuture<RemotingCommand> responseFuture = snodeController.getEnodeService().pullMessage(requestHeader.getEnodeName(), request);
        responseFuture.whenComplete((data, ex) -> {
            if (ex == null) {
                if (this.snodeController.getConsumeMessageInterceptorGroup() != null) {
                    ResponseContext responseContext = new ResponseContext(request, remotingChannel, data);
                    this.snodeController.getSendMessageInterceptorGroup().afterRequest(responseContext);
                }
                remotingChannel.reply(data);
            } else {
                if (this.snodeController.getConsumeMessageInterceptorGroup() != null) {
                    ExceptionContext exceptionContext = new ExceptionContext(request, remotingChannel, ex, null);
                    this.snodeController.getConsumeMessageInterceptorGroup().onException(exceptionContext);
                }
                log.error("Pull message error: {}", ex);
            }
        });
        return null;
    }

    @Override
    public boolean rejectRequest() {
        return false;
    }
}
