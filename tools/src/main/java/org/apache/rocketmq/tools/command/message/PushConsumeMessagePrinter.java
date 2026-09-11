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
package org.apache.rocketmq.tools.command.message;

import java.io.PrintStream;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.TreeMap;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.common.RemotingHelper;

/**
 * Human-readable output for the push-consumer diagnostic command.
 */
public class PushConsumeMessagePrinter {
    private static final String DATE_PATTERN = "yyyy-MM-dd HH:mm:ss.SSS";

    private final PushConsumeMessageConfig config;
    private final PrintStream out;

    public PushConsumeMessagePrinter(PushConsumeMessageConfig config, PrintStream out) {
        this.config = config;
        this.out = out;
    }

    public void printStarted() {
        out.printf("Push consumer started. topic=%s, group=%s, subscription=%s, instance=%s, orderly=%s, trace=%s%n",
            config.getTopic(), config.getConsumerGroup(), config.getSubExpression(), config.getInstanceName(),
            config.isOrderly(), config.isMessageTraceEnabled());
        if (config.getMaxMessages() == 0 && config.getMaxWaitMillis() == 0) {
            out.println("The command will keep consuming until the process is interrupted.");
        } else {
            out.printf("Stop condition: messageCount=%s, maxWaitMillis=%s%n",
                config.getMaxMessages() == 0 ? "unlimited" : Long.toString(config.getMaxMessages()),
                config.getMaxWaitMillis() == 0 ? "unlimited" : Long.toString(config.getMaxWaitMillis()));
        }
    }

    public synchronized void printMessage(long sequence, long receiveTimestamp, MessageQueue messageQueue,
        MessageExt message) {
        byte[] body = message.getBody();
        int bodySize = body == null ? 0 : body.length;
        out.printf("%n#%d receiveTime=%s broker=%s queueId=%d queueOffset=%d%n",
            sequence, formatTimestamp(receiveTimestamp), messageQueue.getBrokerName(), messageQueue.getQueueId(),
            message.getQueueOffset());
        out.printf("msgId=%s keys=%s tags=%s reconsumeTimes=%d bodySize=%d%n",
            valueOrDash(message.getMsgId()), valueOrDash(message.getKeys()), valueOrDash(message.getTags()),
            message.getReconsumeTimes(), bodySize);
        out.printf("bornTime=%s storeTime=%s bornHost=%s storeHost=%s%n",
            formatTimestamp(message.getBornTimestamp()), formatTimestamp(message.getStoreTimestamp()),
            socketAddress(message.getBornHost()), socketAddress(message.getStoreHost()));

        if (config.isPrintProperties()) {
            printProperties(message.getProperties());
        }
        if (config.isPrintBody()) {
            out.printf("body=%s%n", decodeBody(body));
        }
    }

    public void printSummary(PushConsumeMessageObserver.Snapshot snapshot,
        PushConsumeMessageObserver.CompletionReason completionReason) {
        out.printf("%nPush consumer stopped. reason=%s, messages=%d, bodyBytes=%d, elapsedMillis=%d%n",
            completionReason, snapshot.getMessageCount(), snapshot.getBodyBytes(), elapsedMillis(snapshot));
        if (snapshot.getQueueSnapshots().isEmpty()) {
            out.println("No messages were received.");
            return;
        }

        out.printf("%-32s %-8s %-10s %-14s %-14s %s%n", "#Broker", "#Queue", "#Messages", "#FirstOffset",
            "#LastOffset", "#LastStoreTime");
        for (PushConsumeMessageObserver.QueueSnapshot queue : snapshot.getQueueSnapshots()) {
            MessageQueue messageQueue = queue.getMessageQueue();
            out.printf("%-32s %-8d %-10d %-14d %-14d %s%n", messageQueue.getBrokerName(), messageQueue.getQueueId(),
                queue.getMessageCount(), queue.getFirstOffset(), queue.getLastOffset(),
                formatTimestamp(queue.getLastStoreTimestamp()));
        }
    }

    private void printProperties(Map<String, String> properties) {
        if (properties == null || properties.isEmpty()) {
            out.println("properties={}");
            return;
        }

        Map<String, String> visibleProperties = new TreeMap<>(properties);
        visibleProperties.remove(MessageConst.PROPERTY_WAIT_STORE_MSG_OK);
        out.printf("properties=%s%n", visibleProperties);
    }

    private String decodeBody(byte[] body) {
        if (body == null) {
            return "<null>";
        }
        try {
            CharBuffer decoded = config.getCharset().newDecoder()
                .onMalformedInput(CodingErrorAction.REPLACE)
                .onUnmappableCharacter(CodingErrorAction.REPLACE)
                .decode(ByteBuffer.wrap(body));
            return decoded.toString();
        } catch (CharacterCodingException e) {
            return "<unable to decode with " + config.getCharset().name() + ">";
        }
    }

    private long elapsedMillis(PushConsumeMessageObserver.Snapshot snapshot) {
        if (snapshot.getFirstReceiveTimestamp() == 0 || snapshot.getLastReceiveTimestamp() == 0) {
            return 0;
        }
        return Math.max(0, snapshot.getLastReceiveTimestamp() - snapshot.getFirstReceiveTimestamp());
    }

    private String formatTimestamp(long timestamp) {
        if (timestamp <= 0) {
            return "-";
        }
        return new SimpleDateFormat(DATE_PATTERN).format(new Date(timestamp));
    }

    private String valueOrDash(Object value) {
        return value == null || value.toString().isEmpty() ? "-" : value.toString();
    }

    private String socketAddress(SocketAddress address) {
        return valueOrDash(address == null ? null : RemotingHelper.parseSocketAddressAddr(address));
    }
}
