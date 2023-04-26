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

package org.apache.rocketmq.client.impl.admin;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.client.MqClientAdmin;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.RemotingClient;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;
import org.apache.rocketmq.remoting.protocol.RequestCode;
import org.apache.rocketmq.remoting.protocol.ResponseCode;
import org.apache.rocketmq.remoting.protocol.admin.ConsumeStats;
import org.apache.rocketmq.remoting.protocol.admin.TopicStatsTable;
import org.apache.rocketmq.remoting.protocol.body.ClusterInfo;
import org.apache.rocketmq.remoting.protocol.body.ConsumeMessageDirectlyResult;
import org.apache.rocketmq.remoting.protocol.body.ConsumerConnection;
import org.apache.rocketmq.remoting.protocol.body.ConsumerRunningInfo;
import org.apache.rocketmq.remoting.protocol.body.GroupList;
import org.apache.rocketmq.remoting.protocol.body.QueryConsumeTimeSpanBody;
import org.apache.rocketmq.remoting.protocol.body.QuerySubscriptionResponseBody;
import org.apache.rocketmq.remoting.protocol.body.QueueTimeSpan;
import org.apache.rocketmq.remoting.protocol.body.ResetOffsetBody;
import org.apache.rocketmq.remoting.protocol.body.TopicList;
import org.apache.rocketmq.remoting.protocol.header.ConsumeMessageDirectlyResultRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.CreateTopicRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.DeleteSubscriptionGroupRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.DeleteTopicRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.GetConsumeStatsRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.GetConsumerConnectionListRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.GetConsumerRunningInfoRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.GetTopicStatsInfoRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.QueryConsumeTimeSpanRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.QueryMessageRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.QuerySubscriptionByConsumerRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.QueryTopicConsumeByWhoRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.QueryTopicsByConsumerRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.ResetOffsetRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.ViewMessageRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.namesrv.DeleteKVConfigRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.namesrv.DeleteTopicFromNamesrvRequestHeader;
import org.apache.rocketmq.remoting.protocol.heartbeat.SubscriptionData;
import org.apache.rocketmq.remoting.protocol.subscription.SubscriptionGroupConfig;

public class MqClientAdminImpl implements MqClientAdmin {
    private final static Logger log = LoggerFactory.getLogger(MqClientAdminImpl.class);
    private final RemotingClient remotingClient;

    public MqClientAdminImpl(RemotingClient remotingClient) {
        this.remotingClient = remotingClient;
    }

    @Override
    public CompletableFuture<List<MessageExt>> queryMessage(String address, boolean uniqueKeyFlag, boolean decompressBody,
        QueryMessageRequestHeader requestHeader, long timeoutMillis) {
        CompletableFuture<List<MessageExt>> future = new CompletableFuture<>();
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.QUERY_MESSAGE, requestHeader);
        request.addExtField(MixAll.UNIQUE_MSG_QUERY_FLAG, String.valueOf(uniqueKeyFlag));
        remotingClient.invoke(address, request, timeoutMillis).thenAccept(response -> {
            if (response.getCode() == ResponseCode.SUCCESS) {
                List<MessageExt> wrappers = MessageDecoder.decodesBatch(ByteBuffer.wrap(response.getBody()), true, decompressBody, true);
                future.complete(filterMessages(wrappers, requestHeader.getTopic(), requestHeader.getKey(), uniqueKeyFlag));
            } else if (response.getCode() == ResponseCode.QUERY_NOT_FOUND)  {
                List<MessageExt> wrappers = new ArrayList<>();
                future.complete(wrappers);
            } else {
                log.warn("queryMessage getResponseCommand failed, {} {}, header={}", response.getCode(), response.getRemark(), requestHeader);
                future.completeExceptionally(new MQClientException(response.getCode(), response.getRemark()));
            }
        });

        return future;
    }

    @Override
    public CompletableFuture<TopicStatsTable> getTopicStatsInfo(String address,
        GetTopicStatsInfoRequestHeader requestHeader, long timeoutMillis) {
        CompletableFuture<TopicStatsTable> future = new CompletableFuture<>();
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_TOPIC_STATS_INFO, requestHeader);
        remotingClient.invoke(address, request, timeoutMillis).thenAccept(response -> {
            if (response.getCode() == ResponseCode.SUCCESS) {
                TopicStatsTable topicStatsTable = TopicStatsTable.decode(response.getBody(), TopicStatsTable.class);
                future.complete(topicStatsTable);
            } else {
                log.warn("getTopicStatsInfo getResponseCommand failed, {} {}, header={}", response.getCode(), response.getRemark(), requestHeader);
                future.completeExceptionally(new MQClientException(response.getCode(), response.getRemark()));
            }
        });
        return future;
    }

    @Override
    public CompletableFuture<List<QueueTimeSpan>> queryConsumeTimeSpan(String address,
        QueryConsumeTimeSpanRequestHeader requestHeader, long timeoutMillis) {
        CompletableFuture<List<QueueTimeSpan>> future = new CompletableFuture<>();
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.QUERY_CONSUME_TIME_SPAN, requestHeader);
        remotingClient.invoke(address, request, timeoutMillis).thenAccept(response -> {
            if (response.getCode() == ResponseCode.SUCCESS) {
                QueryConsumeTimeSpanBody consumeTimeSpanBody = GroupList.decode(response.getBody(), QueryConsumeTimeSpanBody.class);
                future.complete(consumeTimeSpanBody.getConsumeTimeSpanSet());
            } else {
                log.warn("queryConsumerTimeSpan getResponseCommand failed, {} {}, header={}", response.getCode(), response.getRemark(), requestHeader);
                future.completeExceptionally(new MQClientException(response.getCode(), response.getRemark()));
            }
        });
        return future;
    }

    @Override
    public CompletableFuture<Void> updateOrCreateTopic(String address, CreateTopicRequestHeader requestHeader,
        long timeoutMillis) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.UPDATE_AND_CREATE_TOPIC, requestHeader);
        remotingClient.invoke(address, request, timeoutMillis).thenAccept(response -> {
            if (response.getCode() == ResponseCode.SUCCESS) {
                future.complete(null);
            } else {
                log.warn("updateOrCreateTopic getResponseCommand failed, {} {}, header={}", response.getCode(), response.getRemark(), requestHeader);
                future.completeExceptionally(new MQClientException(response.getCode(), response.getRemark()));
            }
        });
        return future;
    }

    @Override
    public CompletableFuture<Void> updateOrCreateSubscriptionGroup(String address, SubscriptionGroupConfig config,
        long timeoutMillis) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.UPDATE_AND_CREATE_SUBSCRIPTIONGROUP, null);
        byte[] body = RemotingSerializable.encode(config);
        request.setBody(body);
        remotingClient.invoke(address, request, timeoutMillis).thenAccept(response -> {
            if (response.getCode() == ResponseCode.SUCCESS) {
                future.complete(null);
            } else {
                log.warn("updateOrCreateSubscriptionGroup getResponseCommand failed, {} {}, header={}", response.getCode(), response.getRemark(), config);
                future.completeExceptionally(new MQClientException(response.getCode(), response.getRemark()));
            }
        });
        return future;
    }

    @Override
    public CompletableFuture<Void> deleteTopicInBroker(String address, DeleteTopicRequestHeader requestHeader,
        long timeoutMillis) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.DELETE_TOPIC_IN_BROKER, requestHeader);
        remotingClient.invoke(address, request, timeoutMillis).thenAccept(response -> {
            if (response.getCode() == ResponseCode.SUCCESS) {
                future.complete(null);
            } else {
                log.warn("deleteTopicInBroker getResponseCommand failed, {} {}, header={}", response.getCode(), response.getRemark(), requestHeader);
                future.completeExceptionally(new MQClientException(response.getCode(), response.getRemark()));
            }
        });
        return future;
    }

    @Override
    public CompletableFuture<Void> deleteTopicInNameserver(String address, DeleteTopicFromNamesrvRequestHeader requestHeader,
        long timeoutMillis) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.DELETE_TOPIC_IN_NAMESRV, requestHeader);
        remotingClient.invoke(address, request, timeoutMillis).thenAccept(response -> {
            if (response.getCode() == ResponseCode.SUCCESS) {
                future.complete(null);
            } else {
                log.warn("deleteTopicInNameserver getResponseCommand failed, {} {}, header={}", response.getCode(), response.getRemark(), requestHeader);
                future.completeExceptionally(new MQClientException(response.getCode(), response.getRemark()));
            }
        });
        return future;
    }

    @Override
    public CompletableFuture<Void> deleteKvConfig(String address, DeleteKVConfigRequestHeader requestHeader,
        long timeoutMillis) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.DELETE_KV_CONFIG, requestHeader);
        remotingClient.invoke(address, request, timeoutMillis).thenAccept(response -> {
            if (response.getCode() == ResponseCode.SUCCESS) {
                future.complete(null);
            } else {
                log.warn("deleteKvConfig getResponseCommand failed, {} {}, header={}", response.getCode(), response.getRemark(), requestHeader);
                future.completeExceptionally(new MQClientException(response.getCode(), response.getRemark()));
            }
        });
        return future;
    }

    @Override
    public CompletableFuture<Void> deleteSubscriptionGroup(String address, DeleteSubscriptionGroupRequestHeader requestHeader,
        long timeoutMillis) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.DELETE_SUBSCRIPTIONGROUP, requestHeader);
        remotingClient.invoke(address, request, timeoutMillis).thenAccept(response -> {
            if (response.getCode() == ResponseCode.SUCCESS) {
                future.complete(null);
            } else {
                log.warn("deleteSubscriptionGroup getResponseCommand failed, {} {}, header={}", response.getCode(), response.getRemark(), requestHeader);
                future.completeExceptionally(new MQClientException(response.getCode(), response.getRemark()));
            }
        });
        return future;
    }

    @Override
    public CompletableFuture<Map<MessageQueue, Long>> invokeBrokerToResetOffset(String address,
        ResetOffsetRequestHeader requestHeader, long timeoutMillis) {
        CompletableFuture<Map<MessageQueue, Long>> future = new CompletableFuture<>();
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.INVOKE_BROKER_TO_RESET_OFFSET, requestHeader);
        remotingClient.invoke(address, request, timeoutMillis).thenAccept(response -> {
            if (response.getCode() == ResponseCode.SUCCESS && null != response.getBody()) {
                Map<MessageQueue, Long> offsetTable = ResetOffsetBody.decode(response.getBody(), ResetOffsetBody.class).getOffsetTable();
                future.complete(offsetTable);
                log.info("Invoke broker to reset offset success. address:{}, header:{}, offsetTable:{}",
                    address, requestHeader, offsetTable);
            } else {
                log.warn("invokeBrokerToResetOffset getResponseCommand failed, {} {}, header={}", response.getCode(), response.getRemark(), requestHeader);
                future.completeExceptionally(new MQClientException(response.getCode(), response.getRemark()));
            }
        });
        return future;
    }

    @Override
    public CompletableFuture<MessageExt> viewMessage(String address, ViewMessageRequestHeader requestHeader,
        long timeoutMillis) {
        CompletableFuture<MessageExt> future = new CompletableFuture<>();
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.VIEW_MESSAGE_BY_ID, requestHeader);
        remotingClient.invoke(address, request, timeoutMillis).thenAccept(response -> {
            if (response.getCode() == ResponseCode.SUCCESS) {
                ByteBuffer byteBuffer = ByteBuffer.wrap(response.getBody());
                MessageExt messageExt = MessageDecoder.clientDecode(byteBuffer, true);
                future.complete(messageExt);
            } else {
                log.warn("viewMessage getResponseCommand failed, {} {}, header={}", response.getCode(), response.getRemark(), requestHeader);
                future.completeExceptionally(new MQClientException(response.getCode(), response.getRemark()));
            }
        });
        return future;
    }

    @Override
    public CompletableFuture<ClusterInfo> getBrokerClusterInfo(String address, long timeoutMillis) {
        CompletableFuture<ClusterInfo> future = new CompletableFuture<>();
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_BROKER_CLUSTER_INFO, null);
        remotingClient.invoke(address, request, timeoutMillis).thenAccept(response -> {
            if (response.getCode() == ResponseCode.SUCCESS) {
                ClusterInfo clusterInfo = ClusterInfo.decode(response.getBody(), ClusterInfo.class);
                future.complete(clusterInfo);
            } else {
                log.warn("getBrokerClusterInfo getResponseCommand failed, {} {}", response.getCode(), response.getRemark());
                future.completeExceptionally(new MQClientException(response.getCode(), response.getRemark()));
            }
        });
        return future;
    }

    @Override
    public CompletableFuture<ConsumerConnection> getConsumerConnectionList(String address,
        GetConsumerConnectionListRequestHeader requestHeader, long timeoutMillis) {
        CompletableFuture<ConsumerConnection> future = new CompletableFuture<>();
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_CONSUMER_CONNECTION_LIST, requestHeader);
        remotingClient.invoke(address, request, timeoutMillis).thenAccept(response -> {
            if (response.getCode() == ResponseCode.SUCCESS) {
                ConsumerConnection consumerConnection = ConsumerConnection.decode(response.getBody(), ConsumerConnection.class);
                future.complete(consumerConnection);
            } else {
                log.warn("getConsumerConnectionList getResponseCommand failed, {} {}", response.getCode(), response.getRemark());
                future.completeExceptionally(new MQClientException(response.getCode(), response.getRemark()));
            }
        });
        return future;
    }

    @Override
    public CompletableFuture<TopicList> queryTopicsByConsumer(String address,
        QueryTopicsByConsumerRequestHeader requestHeader, long timeoutMillis) {
        CompletableFuture<TopicList> future = new CompletableFuture<>();
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.QUERY_TOPICS_BY_CONSUMER, requestHeader);
        remotingClient.invoke(address, request, timeoutMillis).thenAccept(response -> {
            if (response.getCode() == ResponseCode.SUCCESS) {
                TopicList topicList = TopicList.decode(response.getBody(), TopicList.class);
                future.complete(topicList);
            } else {
                log.warn("queryTopicsByConsumer getResponseCommand failed, {} {}", response.getCode(), response.getRemark());
                future.completeExceptionally(new MQClientException(response.getCode(), response.getRemark()));
            }
        });
        return future;
    }

    @Override
    public CompletableFuture<SubscriptionData> querySubscriptionByConsumer(String address,
        QuerySubscriptionByConsumerRequestHeader requestHeader, long timeoutMillis) {
        CompletableFuture<SubscriptionData> future = new CompletableFuture<>();
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.QUERY_SUBSCRIPTION_BY_CONSUMER, requestHeader);
        remotingClient.invoke(address, request, timeoutMillis).thenAccept(response -> {
            if (response.getCode() == ResponseCode.SUCCESS) {
                QuerySubscriptionResponseBody subscriptionResponseBody =
                    QuerySubscriptionResponseBody.decode(response.getBody(), QuerySubscriptionResponseBody.class);
                future.complete(subscriptionResponseBody.getSubscriptionData());
            } else {
                log.warn("querySubscriptionByConsumer getResponseCommand failed, {} {}", response.getCode(), response.getRemark());
                future.completeExceptionally(new MQClientException(response.getCode(), response.getRemark()));
            }
        });
        return future;
    }

    @Override
    public CompletableFuture<ConsumeStats> getConsumeStats(String address, GetConsumeStatsRequestHeader requestHeader,
        long timeoutMillis) {
        CompletableFuture<ConsumeStats> future = new CompletableFuture<>();
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_CONSUME_STATS, requestHeader);
        remotingClient.invoke(address, request, timeoutMillis).thenAccept(response -> {
            if (response.getCode() == ResponseCode.SUCCESS) {
                ConsumeStats consumeStats = ConsumeStats.decode(response.getBody(), ConsumeStats.class);
                future.complete(consumeStats);
            } else {
                log.warn("getConsumeStats getResponseCommand failed, {} {}", response.getCode(), response.getRemark());
                future.completeExceptionally(new MQClientException(response.getCode(), response.getRemark()));
            }
        });
        return future;
    }

    @Override
    public CompletableFuture<GroupList> queryTopicConsumeByWho(String address,
        QueryTopicConsumeByWhoRequestHeader requestHeader, long timeoutMillis) {
        CompletableFuture<GroupList> future = new CompletableFuture<>();
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.QUERY_TOPIC_CONSUME_BY_WHO, requestHeader);
        remotingClient.invoke(address, request, timeoutMillis).thenAccept(response -> {
            if (response.getCode() == ResponseCode.SUCCESS) {
                GroupList groupList = GroupList.decode(response.getBody(), GroupList.class);
                future.complete(groupList);
            } else {
                log.warn("queryTopicConsumeByWho getResponseCommand failed, {} {}", response.getCode(), response.getRemark());
                future.completeExceptionally(new MQClientException(response.getCode(), response.getRemark()));
            }
        });
        return future;
    }

    @Override
    public CompletableFuture<ConsumerRunningInfo> getConsumerRunningInfo(String address,
        GetConsumerRunningInfoRequestHeader requestHeader, long timeoutMillis) {
        CompletableFuture<ConsumerRunningInfo> future = new CompletableFuture<>();
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_CONSUMER_RUNNING_INFO, requestHeader);
        remotingClient.invoke(address, request, timeoutMillis).thenAccept(response -> {
            if (response.getCode() == ResponseCode.SUCCESS) {
                ConsumerRunningInfo info = ConsumerRunningInfo.decode(response.getBody(), ConsumerRunningInfo.class);
                future.complete(info);
            } else {
                log.warn("getConsumerRunningInfo getResponseCommand failed, {} {}", response.getCode(), response.getRemark());
                future.completeExceptionally(new MQClientException(response.getCode(), response.getRemark()));
            }
        });
        return future;
    }

    @Override
    public CompletableFuture<ConsumeMessageDirectlyResult> consumeMessageDirectly(String address,
        ConsumeMessageDirectlyResultRequestHeader requestHeader, long timeoutMillis) {
        CompletableFuture<ConsumeMessageDirectlyResult> future = new CompletableFuture<>();
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.CONSUME_MESSAGE_DIRECTLY, requestHeader);
        remotingClient.invoke(address, request, timeoutMillis).thenAccept(response -> {
            if (response.getCode() == ResponseCode.SUCCESS) {
                ConsumeMessageDirectlyResult info = ConsumeMessageDirectlyResult.decode(response.getBody(), ConsumeMessageDirectlyResult.class);
                future.complete(info);
            } else {
                log.warn("consumeMessageDirectly getResponseCommand failed, {} {}", response.getCode(), response.getRemark());
                future.completeExceptionally(new MQClientException(response.getCode(), response.getRemark()));
            }
        });
        return future;
    }

    private List<MessageExt> filterMessages(List<MessageExt> messageFoundList, String topic, String key,
        boolean uniqueKeyFlag) {
        List<MessageExt> matchedMessages = new ArrayList<>();
        if (uniqueKeyFlag) {
            matchedMessages.addAll(messageFoundList.stream()
                .filter(msg -> topic.equals(msg.getTopic()))
                .filter(msg -> key.equals(msg.getMsgId()))
                .collect(Collectors.toList())
            );
        } else {
            matchedMessages.addAll(messageFoundList.stream()
                .filter(msg -> topic.equals(msg.getTopic()))
                .filter(msg -> {
                    boolean matched = false;
                    if (StringUtils.isNotBlank(msg.getKeys())) {
                        String[] keyArray = msg.getKeys().split(MessageConst.KEY_SEPARATOR);
                        for (String s : keyArray) {
                            if (key.equals(s)) {
                                matched = true;
                                break;
                            }
                        }
                    }

                    return matched;
                }).collect(Collectors.toList()));
        }

        return matchedMessages;
    }
}
