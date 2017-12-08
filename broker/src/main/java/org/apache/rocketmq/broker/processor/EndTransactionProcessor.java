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
package org.apache.rocketmq.broker.processor;

import io.netty.channel.ChannelHandlerContext;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.common.TopicFilterType;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.protocol.ResponseCode;
import org.apache.rocketmq.common.protocol.header.EndTransactionRequestHeader;
import org.apache.rocketmq.common.sysflag.MessageSysFlag;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.netty.NettyRequestProcessor;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.store.MessageExtBrokerInner;
import org.apache.rocketmq.store.MessageStore;
import org.apache.rocketmq.store.PutMessageResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EndTransactionProcessor implements NettyRequestProcessor {
    private static final Logger LOGGER = LoggerFactory.getLogger(LoggerName.TRANSACTION_LOGGER_NAME);
    private final BrokerController brokerController;

    public EndTransactionProcessor(final BrokerController brokerController) {
        this.brokerController = brokerController;
    }

    @Override
    public RemotingCommand processRequest(ChannelHandlerContext ctx,
        RemotingCommand request) throws RemotingCommandException {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);
        final EndTransactionRequestHeader requestHeader =
            (EndTransactionRequestHeader) request.decodeCommandCustomHeader(EndTransactionRequestHeader.class);

        if (requestHeader.getFromTransactionCheck()) {
            switch (requestHeader.getCommitOrRollback()) {
                case MessageSysFlag.TRANSACTION_NOT_TYPE: {
                    LOGGER.warn("check producer[{}] transaction state, but it's pending status."
                            + "RequestHeader: {} Remark: {}",
                        RemotingHelper.parseChannelRemoteAddr(ctx.channel()),
                        requestHeader.toString(),
                        request.getRemark());
                    return null;
                }

                case MessageSysFlag.TRANSACTION_COMMIT_TYPE: {
                    LOGGER.warn("check producer[{}] transaction state, the producer commit the message."
                            + "RequestHeader: {} Remark: {}",
                        RemotingHelper.parseChannelRemoteAddr(ctx.channel()),
                        requestHeader.toString(),
                        request.getRemark());

                    break;
                }

                case MessageSysFlag.TRANSACTION_ROLLBACK_TYPE: {
                    LOGGER.warn("check producer[{}] transaction state, the producer rollback the message."
                            + "RequestHeader: {} Remark: {}",
                        RemotingHelper.parseChannelRemoteAddr(ctx.channel()),
                        requestHeader.toString(),
                        request.getRemark());
                    break;
                }
                default:
                    return null;
            }
        } else {
            switch (requestHeader.getCommitOrRollback()) {
                case MessageSysFlag.TRANSACTION_NOT_TYPE: {
                    LOGGER.warn("the producer[{}] end transaction in sending message,  and it's pending status."
                            + "RequestHeader: {} Remark: {}",
                        RemotingHelper.parseChannelRemoteAddr(ctx.channel()),
                        requestHeader.toString(),
                        request.getRemark());
                    return null;
                }

                case MessageSysFlag.TRANSACTION_COMMIT_TYPE: {
                    break;
                }

                case MessageSysFlag.TRANSACTION_ROLLBACK_TYPE: {
                    LOGGER.warn("the producer[{}] end transaction in sending message, rollback the message."
                            + "RequestHeader: {} Remark: {}",
                        RemotingHelper.parseChannelRemoteAddr(ctx.channel()),
                        requestHeader.toString(),
                        request.getRemark());
                    break;
                }
                default:
                    return null;
            }
        }

        final MessageExt msgExt = this.brokerController.getMessageStore().lookMessageByOffset(requestHeader.getCommitLogOffset());
        if (msgExt != null) {
            final String pgroupRead = msgExt.getProperty(MessageConst.PROPERTY_PRODUCER_GROUP);
            if (!pgroupRead.equals(requestHeader.getProducerGroup())) {
                response.setCode(ResponseCode.SYSTEM_ERROR);
                response.setRemark("the producer group wrong");
                return response;
            }

            if (msgExt.getQueueOffset() != requestHeader.getTranStateTableOffset()) {
                response.setCode(ResponseCode.SYSTEM_ERROR);
                response.setRemark("the transaction state table offset wrong");
                return response;
            }

            if (msgExt.getCommitLogOffset() != requestHeader.getCommitLogOffset()) {
                response.setCode(ResponseCode.SYSTEM_ERROR);
                response.setRemark("the commit log offset wrong");
                return response;
            }

            MessageExtBrokerInner msgInner = this.endMessageTransaction(msgExt);
            msgInner.setSysFlag(MessageSysFlag.resetTransactionValue(msgInner.getSysFlag(), requestHeader.getCommitOrRollback()));

            msgInner.setQueueOffset(requestHeader.getTranStateTableOffset());
            msgInner.setPreparedTransactionOffset(requestHeader.getCommitLogOffset());
            msgInner.setStoreTimestamp(msgExt.getStoreTimestamp());
            if (MessageSysFlag.TRANSACTION_ROLLBACK_TYPE == requestHeader.getCommitOrRollback()) {
                msgInner.setBody(null);
            }

            final MessageStore messageStore = this.brokerController.getMessageStore();
            final PutMessageResult putMessageResult = messageStore.putMessage(msgInner);
            if (putMessageResult != null) {
                switch (putMessageResult.getPutMessageStatus()) {
                    // Success
                    case PUT_OK:
                    case FLUSH_DISK_TIMEOUT:
                    case FLUSH_SLAVE_TIMEOUT:
                    case SLAVE_NOT_AVAILABLE:
                        response.setCode(ResponseCode.SUCCESS);
                        response.setRemark(null);
                        break;
                    // Failed
                    case CREATE_MAPEDFILE_FAILED:
                        response.setCode(ResponseCode.SYSTEM_ERROR);
                        response.setRemark("create mapped file failed.");
                        break;
                    case MESSAGE_ILLEGAL:
                    case PROPERTIES_SIZE_EXCEEDED:
                        response.setCode(ResponseCode.MESSAGE_ILLEGAL);
                        response.setRemark("the message is illegal, maybe msg body or properties length not matched. msg body length limit 128k, msg properties length limit 32k.");
                        break;
                    case SERVICE_NOT_AVAILABLE:
                        response.setCode(ResponseCode.SERVICE_NOT_AVAILABLE);
                        response.setRemark("service not available now.");
                        break;
                    case OS_PAGECACHE_BUSY:
                        response.setCode(ResponseCode.SYSTEM_ERROR);
                        response.setRemark("OS page cache busy, please try another machine");
                        break;
                    case UNKNOWN_ERROR:
                        response.setCode(ResponseCode.SYSTEM_ERROR);
                        response.setRemark("UNKNOWN_ERROR");
                        break;
                    default:
                        response.setCode(ResponseCode.SYSTEM_ERROR);
                        response.setRemark("UNKNOWN_ERROR DEFAULT");
                        break;
                }

                return response;
            } else {
                response.setCode(ResponseCode.SYSTEM_ERROR);
                response.setRemark("store putMessage return null");
            }
        } else {
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark("find prepared transaction message failed");
            return response;
        }

        return response;
    }

    @Override
    public boolean rejectRequest() {
        return false;
    }

    private MessageExtBrokerInner endMessageTransaction(MessageExt msgExt) {
        MessageExtBrokerInner msgInner = new MessageExtBrokerInner();
        msgInner.setBody(msgExt.getBody());
        msgInner.setFlag(msgExt.getFlag());
        MessageAccessor.setProperties(msgInner, msgExt.getProperties());

        TopicFilterType topicFilterType =
            (msgInner.getSysFlag() & MessageSysFlag.MULTI_TAGS_FLAG) == MessageSysFlag.MULTI_TAGS_FLAG ? TopicFilterType.MULTI_TAG
                : TopicFilterType.SINGLE_TAG;
        long tagsCodeValue = MessageExtBrokerInner.tagsString2tagsCode(topicFilterType, msgInner.getTags());
        msgInner.setTagsCode(tagsCodeValue);
        msgInner.setPropertiesString(MessageDecoder.messageProperties2String(msgExt.getProperties()));

        msgInner.setSysFlag(msgExt.getSysFlag());
        msgInner.setBornTimestamp(msgExt.getBornTimestamp());
        msgInner.setBornHost(msgExt.getBornHost());
        msgInner.setStoreHost(msgExt.getStoreHost());
        msgInner.setReconsumeTimes(msgExt.getReconsumeTimes());

        msgInner.setWaitStoreMsgOK(false);
        MessageAccessor.clearProperty(msgInner, MessageConst.PROPERTY_DELAY_TIME_LEVEL);

        msgInner.setTopic(msgExt.getTopic());
        msgInner.setQueueId(msgExt.getQueueId());

        return msgInner;
    }
}
