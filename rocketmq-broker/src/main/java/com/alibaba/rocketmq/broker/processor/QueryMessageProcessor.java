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
package com.alibaba.rocketmq.broker.processor;

import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.FileRegion;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.rocketmq.broker.BrokerController;
import com.alibaba.rocketmq.broker.pagecache.OneMessageTransfer;
import com.alibaba.rocketmq.broker.pagecache.QueryMessageTransfer;
import com.alibaba.rocketmq.common.constant.LoggerName;
import com.alibaba.rocketmq.common.protocol.RequestCode;
import com.alibaba.rocketmq.common.protocol.ResponseCode;
import com.alibaba.rocketmq.common.protocol.header.QueryMessageRequestHeader;
import com.alibaba.rocketmq.common.protocol.header.QueryMessageResponseHeader;
import com.alibaba.rocketmq.common.protocol.header.ViewMessageRequestHeader;
import com.alibaba.rocketmq.remoting.exception.RemotingCommandException;
import com.alibaba.rocketmq.remoting.netty.NettyRequestProcessor;
import com.alibaba.rocketmq.remoting.protocol.RemotingCommand;
import com.alibaba.rocketmq.store.QueryMessageResult;
import com.alibaba.rocketmq.store.SelectMapedBufferResult;


/**
 * 查询消息请求处理
 * 
 * @author shijia.wxr<vintage.wang@gmail.com>
 * @since 2013-7-26
 */
public class QueryMessageProcessor implements NettyRequestProcessor {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.BrokerLoggerName);

    private final BrokerController brokerController;


    public QueryMessageProcessor(final BrokerController brokerController) {
        this.brokerController = brokerController;
    }


    @Override
    public RemotingCommand processRequest(ChannelHandlerContext ctx, RemotingCommand request)
            throws RemotingCommandException {
        switch (request.getCode()) {
        case RequestCode.QUERY_MESSAGE:
            return this.queryMessage(ctx, request);
        case RequestCode.VIEW_MESSAGE_BY_ID:
            return this.viewMessageById(ctx, request);
        default:
            break;
        }

        return null;
    }


    public RemotingCommand queryMessage(ChannelHandlerContext ctx, RemotingCommand request)
            throws RemotingCommandException {
        final RemotingCommand response =
                RemotingCommand.createResponseCommand(QueryMessageResponseHeader.class);
        final QueryMessageResponseHeader responseHeader =
                (QueryMessageResponseHeader) response.readCustomHeader();
        final QueryMessageRequestHeader requestHeader =
                (QueryMessageRequestHeader) request
                    .decodeCommandCustomHeader(QueryMessageRequestHeader.class);

        // 由于使用sendfile，所以必须要设置
        response.setOpaque(request.getOpaque());

        final QueryMessageResult queryMessageResult =
                this.brokerController.getMessageStore().queryMessage(requestHeader.getTopic(),
                    requestHeader.getKey(), requestHeader.getMaxNum(), requestHeader.getBeginTimestamp(),
                    requestHeader.getEndTimestamp());
        assert queryMessageResult != null;

        responseHeader.setIndexLastUpdatePhyoffset(queryMessageResult.getIndexLastUpdatePhyoffset());
        responseHeader.setIndexLastUpdateTimestamp(queryMessageResult.getIndexLastUpdateTimestamp());

        // 说明找到消息
        if (queryMessageResult.getBufferTotalSize() > 0) {
            response.setCode(ResponseCode.SUCCESS);
            response.setRemark(null);

            try {
                FileRegion fileRegion =
                        new QueryMessageTransfer(response.encodeHeader(queryMessageResult
                            .getBufferTotalSize()), queryMessageResult);
                ctx.channel().writeAndFlush(fileRegion).addListener(new ChannelFutureListener() {
                    @Override
                    public void operationComplete(ChannelFuture future) throws Exception {
                        queryMessageResult.release();
                        if (!future.isSuccess()) {
                            log.error("transfer query message by pagecache failed, ", future.cause());
                        }
                    }
                });
            }
            catch (Throwable e) {
                log.error("", e);
                queryMessageResult.release();
            }

            return null;
        }

        response.setCode(ResponseCode.QUERY_NOT_FOUND);
        response.setRemark("can not find message, maybe time range not correct");
        return response;
    }


    public RemotingCommand viewMessageById(ChannelHandlerContext ctx, RemotingCommand request)
            throws RemotingCommandException {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);
        final ViewMessageRequestHeader requestHeader =
                (ViewMessageRequestHeader) request.decodeCommandCustomHeader(ViewMessageRequestHeader.class);

        // 由于使用sendfile，所以必须要设置
        response.setOpaque(request.getOpaque());

        final SelectMapedBufferResult selectMapedBufferResult =
                this.brokerController.getMessageStore().selectOneMessageByOffset(requestHeader.getOffset());
        if (selectMapedBufferResult != null) {
            response.setCode(ResponseCode.SUCCESS);
            response.setRemark(null);

            try {
                FileRegion fileRegion =
                        new OneMessageTransfer(response.encodeHeader(selectMapedBufferResult.getSize()),
                            selectMapedBufferResult);
                ctx.channel().writeAndFlush(fileRegion).addListener(new ChannelFutureListener() {
                    @Override
                    public void operationComplete(ChannelFuture future) throws Exception {
                        selectMapedBufferResult.release();
                        if (!future.isSuccess()) {
                            log.error("transfer one message by pagecache failed, ", future.cause());
                        }
                    }
                });
            }
            catch (Throwable e) {
                log.error("", e);
                selectMapedBufferResult.release();
            }

            return null;
        }
        else {
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark("can not find message by the offset, " + requestHeader.getOffset());
        }

        return response;
    }
}
