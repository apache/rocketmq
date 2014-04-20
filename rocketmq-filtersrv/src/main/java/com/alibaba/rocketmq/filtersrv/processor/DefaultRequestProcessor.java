/**
 * Copyright (C) 2010-2013 Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.rocketmq.filtersrv.processor;

import io.netty.channel.ChannelHandlerContext;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.rocketmq.common.constant.LoggerName;
import com.alibaba.rocketmq.common.protocol.RequestCode;
import com.alibaba.rocketmq.common.protocol.ResponseCode;
import com.alibaba.rocketmq.common.protocol.header.filtersrv.RegisterMessageFilterClassRequestHeader;
import com.alibaba.rocketmq.filtersrv.FiltersrvController;
import com.alibaba.rocketmq.remoting.common.RemotingHelper;
import com.alibaba.rocketmq.remoting.exception.RemotingCommandException;
import com.alibaba.rocketmq.remoting.netty.NettyRequestProcessor;
import com.alibaba.rocketmq.remoting.protocol.RemotingCommand;


/**
 * Name Server网络请求处理
 * 
 * @author shijia.wxr<vintage.wang@gmail.com>
 * @since 2013-7-5
 */
public class DefaultRequestProcessor implements NettyRequestProcessor {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.NamesrvLoggerName);

    private final FiltersrvController filtersrvController;


    public DefaultRequestProcessor(FiltersrvController filtersrvController) {
        this.filtersrvController = filtersrvController;
    }


    @Override
    public RemotingCommand processRequest(ChannelHandlerContext ctx, RemotingCommand request)
            throws RemotingCommandException {
        if (log.isDebugEnabled()) {
            log.debug("receive request, {} {} {}",//
                request.getCode(), //
                RemotingHelper.parseChannelRemoteAddr(ctx.channel()), //
                request);
        }

        switch (request.getCode()) {
        case RequestCode.REGISTER_MESSAGE_FILTER_CLASS:
            return registerMessageFilterClass(ctx, request);
        case RequestCode.PULL_MESSAGE:
            return pullMessageForward(ctx, request);
        }

        return null;
    }


    private RemotingCommand pullMessageForward(final ChannelHandlerContext ctx, final RemotingCommand request) {
        return null;
    }


    private RemotingCommand registerMessageFilterClass(ChannelHandlerContext ctx, RemotingCommand request)
            throws RemotingCommandException {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);
        final RegisterMessageFilterClassRequestHeader requestHeader =
                (RegisterMessageFilterClassRequestHeader) request
                    .decodeCommandCustomHeader(RegisterMessageFilterClassRequestHeader.class);

        try {
            boolean ok =
                    this.filtersrvController.getFilterClassManager().registerFilterClass(
                        requestHeader.getConsumerGroup(),//
                        requestHeader.getTopic(),//
                        requestHeader.getClassName(),//
                        requestHeader.getClassCRC(), //
                        request.getBody());// Body传输的是Java Source，必须UTF-8编码
            if (!ok) {
                throw new Exception("registerFilterClass error");
            }
        }
        catch (Exception e) {
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark(RemotingHelper.exceptionSimpleDesc(e));
            return response;
        }

        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        return response;
    }
}
