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

package org.apache.rocketmq.proxy.remoting.activity;

import io.netty.channel.ChannelHandlerContext;
import java.util.HashMap;
import java.util.Map;
import org.apache.rocketmq.acl.common.AclException;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.utils.ExceptionUtils;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.common.ProxyException;
import org.apache.rocketmq.proxy.common.ProxyExceptionCode;
import org.apache.rocketmq.proxy.config.ConfigurationManager;
import org.apache.rocketmq.proxy.config.ProxyConfig;
import org.apache.rocketmq.proxy.processor.MessagingProcessor;
import org.apache.rocketmq.proxy.remoting.pipeline.RequestPipeline;
import org.apache.rocketmq.remoting.netty.NettyRequestProcessor;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.RemotingSysResponseCode;
import org.apache.rocketmq.remoting.protocol.RequestCode;
import org.apache.rocketmq.remoting.protocol.ResponseCode;

public abstract class AbstractRemotingActivity implements NettyRequestProcessor {
    protected final static Logger log = LoggerFactory.getLogger(LoggerName.PROXY_LOGGER_NAME);
    protected final MessagingProcessor messagingProcessor;
    protected static final String BROKER_NAME_FIELD = "bname";
    protected static final String BROKER_NAME_FIELD_FOR_SEND_MESSAGE_V2 = "n";
    private static final Map<ProxyExceptionCode, Integer> PROXY_EXCEPTION_RESPONSE_CODE_MAP = new HashMap<>(8);
    static {
        PROXY_EXCEPTION_RESPONSE_CODE_MAP.put(ProxyExceptionCode.FORBIDDEN, ResponseCode.NO_PERMISSION);
        PROXY_EXCEPTION_RESPONSE_CODE_MAP.put(ProxyExceptionCode.MESSAGE_PROPERTY_CONFLICT_WITH_TYPE, ResponseCode.MESSAGE_ILLEGAL);
        PROXY_EXCEPTION_RESPONSE_CODE_MAP.put(ProxyExceptionCode.INTERNAL_SERVER_ERROR, RemotingSysResponseCode.SYSTEM_ERROR);
        PROXY_EXCEPTION_RESPONSE_CODE_MAP.put(ProxyExceptionCode.TRANSACTION_DATA_NOT_FOUND, RemotingSysResponseCode.SUCCESS);
    }
    protected final RequestPipeline requestPipeline;

    public AbstractRemotingActivity(RequestPipeline requestPipeline, MessagingProcessor messagingProcessor) {
        this.requestPipeline = requestPipeline;
        this.messagingProcessor = messagingProcessor;
    }

    protected RemotingCommand request(ChannelHandlerContext ctx, RemotingCommand request,
        ProxyContext context, long timeoutMillis) throws Exception {
        String brokerName;
        if (request.getCode() == RequestCode.SEND_MESSAGE_V2) {
            if (request.getExtFields().get(BROKER_NAME_FIELD_FOR_SEND_MESSAGE_V2) == null) {
                return RemotingCommand.buildErrorResponse(ResponseCode.VERSION_NOT_SUPPORTED,
                    "Request doesn't have field bname");
            }
            brokerName = request.getExtFields().get(BROKER_NAME_FIELD_FOR_SEND_MESSAGE_V2);
        } else {
            if (request.getExtFields().get(BROKER_NAME_FIELD) == null) {
                return RemotingCommand.buildErrorResponse(ResponseCode.VERSION_NOT_SUPPORTED,
                    "Request doesn't have field bname");
            }
            brokerName = request.getExtFields().get(BROKER_NAME_FIELD);
        }
        if (request.isOnewayRPC()) {
            messagingProcessor.requestOneway(context, brokerName, request, timeoutMillis);
            return null;
        }
        messagingProcessor.request(context, brokerName, request, timeoutMillis)
            .thenAccept(r -> writeResponse(ctx, context, request, r))
            .exceptionally(t -> {
                writeErrResponse(ctx, context, request, t);
                return null;
            });
        return null;
    }

    @Override
    public RemotingCommand processRequest(ChannelHandlerContext ctx, RemotingCommand request) throws Exception {
        ProxyContext context = createContext();
        try {
            this.requestPipeline.execute(ctx, request, context);
            RemotingCommand response = this.processRequest0(ctx, request, context);
            if (response != null) {
                writeResponse(ctx, context, request, response);
            }
            return null;
        } catch (Throwable t) {
            writeErrResponse(ctx, context, request, t);
            return null;
        }
    }

    @Override
    public boolean rejectRequest() {
        return false;
    }

    protected abstract RemotingCommand processRequest0(ChannelHandlerContext ctx, RemotingCommand request,
        ProxyContext context) throws Exception;

    protected ProxyContext createContext() {
        return ProxyContext.create();
    }

    protected void writeErrResponse(ChannelHandlerContext ctx, final ProxyContext context,
        final RemotingCommand request, Throwable t) {
        t = ExceptionUtils.getRealException(t);
        if (t instanceof ProxyException) {
            ProxyException e = (ProxyException) t;
            writeResponse(ctx, context, request,
                RemotingCommand.createResponseCommand(
                    PROXY_EXCEPTION_RESPONSE_CODE_MAP.getOrDefault(e.getCode(), ResponseCode.SYSTEM_ERROR),
                    e.getMessage()),
                t);
        } else if (t instanceof MQClientException) {
            MQClientException e = (MQClientException) t;
            writeResponse(ctx, context, request, RemotingCommand.createResponseCommand(e.getResponseCode(), e.getErrorMessage()), t);
        } else if (t instanceof MQBrokerException) {
            MQBrokerException e = (MQBrokerException) t;
            writeResponse(ctx, context, request, RemotingCommand.createResponseCommand(e.getResponseCode(), e.getErrorMessage()), t);
        } else if (t instanceof AclException) {
            writeResponse(ctx, context, request, RemotingCommand.createResponseCommand(ResponseCode.NO_PERMISSION, t.getMessage()), t);
        } else {
            writeResponse(ctx, context, request,
                RemotingCommand.createResponseCommand(ResponseCode.SYSTEM_ERROR, t.getMessage()), t);
        }
    }

    protected void writeResponse(ChannelHandlerContext ctx, final ProxyContext context,
        final RemotingCommand request, RemotingCommand response) {
        writeResponse(ctx, context, request, response, null);
    }

    protected void writeResponse(ChannelHandlerContext ctx, final ProxyContext context,
        final RemotingCommand request, RemotingCommand response, Throwable t) {
        if (request.isOnewayRPC()) {
            return;
        }
        if (!ctx.channel().isWritable()) {
            return;
        }

        ProxyConfig config = ConfigurationManager.getProxyConfig();

        response.setOpaque(request.getOpaque());
        response.markResponseType();
        response.addExtField(MessageConst.PROPERTY_MSG_REGION, config.getRegionId());
        response.addExtField(MessageConst.PROPERTY_TRACE_SWITCH, String.valueOf(config.isTraceOn()));
        if (t != null) {
            response.setRemark(t.getMessage());
        }

        ctx.writeAndFlush(response);
    }
}
