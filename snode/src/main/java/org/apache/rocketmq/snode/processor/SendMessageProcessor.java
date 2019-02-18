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
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.protocol.RequestCode;
import org.apache.rocketmq.common.protocol.ResponseCode;
import org.apache.rocketmq.common.protocol.header.ConsumerSendMsgBackRequestHeader;
import org.apache.rocketmq.common.protocol.header.SendMessageRequestHeaderV2;
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
import org.apache.rocketmq.snode.service.MetricsService;

public class SendMessageProcessor implements RequestProcessor {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.SNODE_LOGGER_NAME);

    private SnodeController snodeController;

    public SendMessageProcessor(final SnodeController snodeController) {
        this.snodeController = snodeController;
    }

    @Override
    public RemotingCommand processRequest(RemotingChannel remotingChannel,
        RemotingCommand request) throws RemotingCommandException {
        this.snodeController.getMetricsService().incRequestCount(request.getCode(), true);
        try {
            processSendMessageRequest(remotingChannel, request);
        } catch (Exception ex) {
            this.snodeController.getMetricsService().incRequestCount(request.getCode(), false);
            throw ex;
        }
        return null;
    }

    private void processSendMessageRequest(RemotingChannel remotingChannel,
        RemotingCommand request) throws RemotingCommandException {
        MetricsService.Timer timer = this.snodeController.getMetricsService().startTimer(request.getCode());
        if (this.snodeController.getSendMessageInterceptorGroup() != null) {
            RequestContext requestContext = new RequestContext(request, remotingChannel);
            this.snodeController.getSendMessageInterceptorGroup().beforeRequest(requestContext);
        }
        String enodeName;
        SendMessageRequestHeaderV2 sendMessageRequestHeaderV2 = null;
        final StringBuffer stringBuffer = new StringBuffer();
        ConsumerSendMsgBackRequestHeader consumerSendMsgBackRequestHeader = null;
        boolean isSendBack = false;
        if (request.getCode() == RequestCode.SEND_MESSAGE_V2 ||
            request.getCode() == RequestCode.SEND_BATCH_MESSAGE) {
            sendMessageRequestHeaderV2 = (SendMessageRequestHeaderV2) request.decodeCommandCustomHeader(SendMessageRequestHeaderV2.class);
            enodeName = sendMessageRequestHeaderV2.getN();
            stringBuffer.append(sendMessageRequestHeaderV2.getB());
        } else {
            isSendBack = true;
            consumerSendMsgBackRequestHeader = (ConsumerSendMsgBackRequestHeader) request.decodeCommandCustomHeader(ConsumerSendMsgBackRequestHeader.class);
            enodeName = consumerSendMsgBackRequestHeader.getEnodeName();
            stringBuffer.append(MixAll.getRetryTopic(consumerSendMsgBackRequestHeader.getGroup()));
        }

        CompletableFuture<RemotingCommand> responseFuture = snodeController.getEnodeService().sendMessage(enodeName, request);
        final Integer queueId = sendMessageRequestHeaderV2.getE();
        final byte[] message = request.getBody();
        final boolean isNeedPush = !isSendBack;
        responseFuture.whenComplete((data, ex) -> {
            if (ex == null) {
                if (this.snodeController.getSendMessageInterceptorGroup() != null) {
                    ResponseContext responseContext = new ResponseContext(request, remotingChannel, data);
                    this.snodeController.getSendMessageInterceptorGroup().afterRequest(responseContext);
                }
                remotingChannel.reply(data);
                this.snodeController.getMetricsService().recordRequestSize(stringBuffer.toString(), request.getBody().length);
                if (data.getCode() == ResponseCode.SUCCESS && isNeedPush) {
                    this.snodeController.getPushService().pushMessage(enodeName, stringBuffer.toString(), queueId, message, data);
                }
            } else {
                this.snodeController.getMetricsService().incRequestCount(request.getCode(), false);
                if (this.snodeController.getSendMessageInterceptorGroup() != null) {
                    ExceptionContext exceptionContext = new ExceptionContext(request, remotingChannel, ex, null);
                    this.snodeController.getSendMessageInterceptorGroup().onException(exceptionContext);
                }
                log.error("Send Message error: {}", ex);
            }
            timer.observeDuration();
        });
    }

    @Override
    public boolean rejectRequest() {
        return false;
    }
}
