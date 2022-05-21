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

package org.apache.rocketmq.thinclient.metrics;

import com.google.protobuf.Timestamp;
import com.google.protobuf.util.Timestamps;
import io.github.aliyunmq.shaded.org.slf4j.Logger;
import io.github.aliyunmq.shaded.org.slf4j.LoggerFactory;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.DoubleHistogram;
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.metrics.Meter;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import org.apache.rocketmq.apis.consumer.PushConsumer;
import org.apache.rocketmq.thinclient.hook.MessageHookPoints;
import org.apache.rocketmq.thinclient.hook.MessageHookPointsStatus;
import org.apache.rocketmq.thinclient.hook.MessageInterceptor;
import org.apache.rocketmq.thinclient.impl.ClientImpl;
import org.apache.rocketmq.thinclient.message.MessageCommon;

public class MetricMessageInterceptor implements MessageInterceptor {
    private static final Logger LOGGER = LoggerFactory.getLogger(MetricMessageInterceptor.class);

    private final MessageMeter messageMeter;

    public MetricMessageInterceptor(MessageMeter messageMeter) {
        this.messageMeter = messageMeter;
    }

    private void doAfterSendMessage(List<MessageCommon> messageCommons, Duration duration,
        MessageHookPointsStatus status) {
        final LongCounter sendSuccessCounter = messageMeter.getSendSuccessTotalCounter();
        final LongCounter sendFailureCounter = messageMeter.getSendFailureTotalCounter();
        final DoubleHistogram costTimeHistogram = messageMeter.getSendSuccessCostTimeHistogram();
        for (MessageCommon messageCommon : messageCommons) {
            Attributes attributes = Attributes.builder().put(RocketmqAttributes.TOPIC, messageCommon.getTopic())
                .put(RocketmqAttributes.CLIENT_ID, messageMeter.getClient().getClientId()).build();
            if (MessageHookPointsStatus.OK.equals(status)) {
                sendSuccessCounter.add(1, attributes);
                costTimeHistogram.record(duration.toMillis(), attributes);
            }
            if (MessageHookPointsStatus.ERROR.equals(status)) {
                sendFailureCounter.add(1, attributes);
            }
        }
    }

    private void doAfterReceive(List<MessageCommon> messageCommons, MessageHookPointsStatus status) {
        if (messageCommons.isEmpty()) {
            return;
        }
        final Optional<String> optionalConsumerGroup = messageMeter.tryGetConsumerGroup();
        if (!optionalConsumerGroup.isPresent()) {
            LOGGER.error("[Bug] consumerGroup is not recognized, clientId={}", messageMeter.getClient());
            return;
        }
        if (!MessageHookPointsStatus.ERROR.equals(status)) {
            return;
        }
        final String consumerGroup = optionalConsumerGroup.get();
        final MessageCommon messageCommon = messageCommons.iterator().next();
        final Optional<Timestamp> optionalDeliveryTimestampFromRemote = messageCommon.getDeliveryTimestampFromRemote();
        if (!optionalDeliveryTimestampFromRemote.isPresent()) {
            return;
        }
        final Timestamp deliveryTimestampFromRemote = optionalDeliveryTimestampFromRemote.get();
        final long latency = System.currentTimeMillis() - Timestamps.toMillis(deliveryTimestampFromRemote);
        final DoubleHistogram messageDeliveryLatencyHistogram = messageMeter.getMessageDeliveryLatencyHistogram();
        final Attributes attributes = Attributes.builder().put(RocketmqAttributes.TOPIC, messageCommon.getTopic())
            .put(RocketmqAttributes.CONSUMER_GROUP, consumerGroup)
            .put(RocketmqAttributes.CLIENT_ID, messageMeter.getClient().getClientId()).build();
        messageDeliveryLatencyHistogram.record(latency, attributes);
    }

    private void doBeforeConsumeMessage(List<MessageCommon> messageCommons) {
        final Optional<String> optionalConsumerGroup = messageMeter.tryGetConsumerGroup();
        if (!optionalConsumerGroup.isPresent()) {
            LOGGER.error("[Bug] consumerGroup is not recognized, clientId={}", messageMeter.getClient());
            return;
        }
        final String consumerGroup = optionalConsumerGroup.get();
        final MessageCommon messageCommon = messageCommons.iterator().next();
        final Optional<Duration> optionalDurationAfterDecoding = messageCommon.getDurationAfterDecoding();
        if (!optionalDurationAfterDecoding.isPresent()) {
            return;
        }
        final Duration durationAfterDecoding = optionalDurationAfterDecoding.get();
        Attributes attributes = Attributes.builder().put(RocketmqAttributes.TOPIC, messageCommon.getTopic())
            .put(RocketmqAttributes.CONSUMER_GROUP, consumerGroup)
            .put(RocketmqAttributes.CLIENT_ID, messageMeter.getClient().getClientId()).build();
        final DoubleHistogram histogram = messageMeter.getMessageAwaitTimeHistogram();
        histogram.record(durationAfterDecoding.toMillis(), attributes);
    }

    private void doAfterProcessMessage(List<MessageCommon> messageCommons, Duration duration,
        MessageHookPointsStatus status) {
        final LongCounter processSuccessCounter = messageMeter.getProcessSuccessTotalCounter();
        final LongCounter processFailureCounter = messageMeter.getProcessFailureTotalCounter();
        final DoubleHistogram processCostTimeHistogram = messageMeter.getProcessCostTimeHistogram();
        final ClientImpl client = messageMeter.getClient();
        if (!(client instanceof PushConsumer)) {
            // Should never reach here.
            LOGGER.error("[Bug] current client is not push consumer, clientId={}", client.getClientId());
            return;
        }
        PushConsumer pushConsumer = (PushConsumer) client;
        for (MessageCommon messageCommon : messageCommons) {
            Attributes attributes = Attributes.builder().put(RocketmqAttributes.TOPIC, messageCommon.getTopic())
                .put(RocketmqAttributes.CONSUMER_GROUP, pushConsumer.getConsumerGroup())
                .put(RocketmqAttributes.CLIENT_ID, messageMeter.getClient().getClientId()).build();
            if (MessageHookPointsStatus.OK.equals(status)) {
                processSuccessCounter.add(1, attributes);
                processCostTimeHistogram.record(duration.toMillis(), attributes);
            }
            if (MessageHookPointsStatus.ERROR.equals(status)) {
                processFailureCounter.add(1, attributes);
                processCostTimeHistogram.record(duration.toMillis(), attributes);
            }
        }
    }

    private void doAfterAckMessage(List<MessageCommon> messageCommons, MessageHookPointsStatus status) {
        final LongCounter ackSuccessCounter = messageMeter.getAckSuccessTotalCounter();
        final LongCounter ackFailureCounter = messageMeter.getAckFailureTotalCounter();
        final Optional<String> optionalConsumerGroup = messageMeter.tryGetConsumerGroup();
        if (!optionalConsumerGroup.isPresent()) {
            LOGGER.error("[Bug] consumerGroup is not recognized, clientId={}", messageMeter.getClient());
            return;
        }
        final String consumerGroup = optionalConsumerGroup.get();
        for (MessageCommon messageCommon : messageCommons) {
            Attributes attributes = Attributes.builder().put(RocketmqAttributes.TOPIC, messageCommon.getTopic())
                .put(RocketmqAttributes.CONSUMER_GROUP, consumerGroup)
                .put(RocketmqAttributes.CLIENT_ID, messageMeter.getClient().getClientId()).build();
            if (MessageHookPointsStatus.OK.equals(status)) {
                ackSuccessCounter.add(1, attributes);
            }
            if (MessageHookPointsStatus.ERROR.equals(status)) {
                ackFailureCounter.add(1, attributes);
            }
        }
    }

    private void doAfterChangInvisibleDuration(List<MessageCommon> messageCommons, MessageHookPointsStatus status) {
        final LongCounter changeInvisibleDurationSuccessCounter = messageMeter.getChangeInvisibleDurationSuccessCounter();
        final LongCounter changeInvisibleDurationFailureCounter = messageMeter.getChangeInvisibleDurationFailureCounter();
        final Optional<String> optionalConsumerGroup = messageMeter.tryGetConsumerGroup();
        if (!optionalConsumerGroup.isPresent()) {
            LOGGER.error("[Bug] consumerGroup is not recognized, clientId={}", messageMeter.getClient());
            return;
        }
        final String consumerGroup = optionalConsumerGroup.get();
        for (MessageCommon message : messageCommons) {
            Attributes attributes = Attributes.builder().put(RocketmqAttributes.TOPIC, message.getTopic())
                .put(RocketmqAttributes.CONSUMER_GROUP, consumerGroup)
                .put(RocketmqAttributes.CLIENT_ID, messageMeter.getClient().getClientId()).build();
            if (MessageHookPointsStatus.OK.equals(status)) {
                changeInvisibleDurationSuccessCounter.add(1, attributes);
            }
            if (MessageHookPointsStatus.ERROR.equals(status)) {
                changeInvisibleDurationFailureCounter.add(1, attributes);
            }
        }
    }

    @Override
    public void doBefore(MessageHookPoints messageHookPoints, List<MessageCommon> messageCommons) {
        final Meter meter = messageMeter.getMeter();
        if (null == meter) {
            return;
        }
        if (MessageHookPoints.CONSUME.equals(messageHookPoints)) {
            doBeforeConsumeMessage(messageCommons);
        }
    }

    @Override
    public void doAfter(MessageHookPoints messageHookPoints, List<MessageCommon> messageCommons, Duration duration,
        MessageHookPointsStatus status) {
        final Meter meter = messageMeter.getMeter();
        if (null == meter) {
            return;
        }
        switch (messageHookPoints) {
            case SEND:
                doAfterSendMessage(messageCommons, duration, status);
                break;
            case RECEIVE:
                doAfterReceive(messageCommons, status);
                break;
            case CONSUME:
                doAfterProcessMessage(messageCommons, duration, status);
                break;
            case ACK:
                doAfterAckMessage(messageCommons, status);
                break;
            case CHANGE_INVISIBLE_DURATION:
                doAfterChangInvisibleDuration(messageCommons, status);
                break;
            default:
                break;
        }
    }
}
