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
package com.alibaba.rocketmq.broker.client.net;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.FileRegion;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.rocketmq.broker.BrokerController;
import com.alibaba.rocketmq.broker.pagecache.OneMessageTransfer;
import com.alibaba.rocketmq.common.constant.LoggerName;
import com.alibaba.rocketmq.common.protocol.MQProtos.MQRequestCode;
import com.alibaba.rocketmq.common.protocol.header.CheckTransactionStateRequestHeader;
import com.alibaba.rocketmq.common.protocol.header.NotifyConsumerIdsChangedRequestHeader;
import com.alibaba.rocketmq.remoting.protocol.RemotingCommand;
import com.alibaba.rocketmq.store.SelectMapedBufferResult;


/**
 * Broker主动调用客户端接口
 * 
 * @author shijia.wxr<vintage.wang@gmail.com>
 * @since 2013-7-26
 */
public class Broker2Client {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.BrokerLoggerName);
    private final BrokerController brokerController;


    public Broker2Client(BrokerController brokerController) {
        this.brokerController = brokerController;
    }


    /**
     * Broker主动回查Producer事务状态，Oneway
     */
    public void checkProducerTransactionState(//
            final Channel channel,//
            final CheckTransactionStateRequestHeader requestHeader,//
            final SelectMapedBufferResult selectMapedBufferResult//
    ) {
        RemotingCommand request =
                RemotingCommand.createRequestCommand(MQRequestCode.CHECK_TRANSACTION_STATE_VALUE,
                    requestHeader);
        request.markOnewayRPC();

        try {
            FileRegion fileRegion =
                    new OneMessageTransfer(request.encodeHeader(selectMapedBufferResult.getSize()),
                        selectMapedBufferResult);
            channel.writeAndFlush(fileRegion).addListener(new ChannelFutureListener() {
                @Override
                public void operationComplete(ChannelFuture future) throws Exception {
                    selectMapedBufferResult.release();
                    if (!future.isSuccess()) {
                        log.error("invokeProducer failed,", future.cause());
                    }
                }
            });
        }
        catch (Throwable e) {
            log.error("invokeProducer exception", e);
            selectMapedBufferResult.release();
        }
    }


    /**
     * Broker主动通知Consumer，Id列表发生变化，Oneway
     */
    public void notifyConsumerIdsChanged(//
            final Channel channel,//
            final String consumerGroup//
    ) {
        NotifyConsumerIdsChangedRequestHeader requestHeader = new NotifyConsumerIdsChangedRequestHeader();
        requestHeader.setConsumerGroup(consumerGroup);
        RemotingCommand request =
                RemotingCommand.createRequestCommand(MQRequestCode.NOTIFY_CONSUMER_IDS_CHANGED_VALUE,
                    requestHeader);

        try {
            this.brokerController.getRemotingServer().invokeOneway(channel, request, 1000);
        }
        catch (Exception e) {
            log.error("notifyConsumerIdsChanged exception, " + consumerGroup, e);
        }
    }
}
