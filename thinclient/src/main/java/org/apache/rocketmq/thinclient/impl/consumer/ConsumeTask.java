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

package org.apache.rocketmq.thinclient.impl.consumer;

import com.google.common.base.Stopwatch;
import io.github.aliyunmq.shaded.org.slf4j.Logger;
import io.github.aliyunmq.shaded.org.slf4j.LoggerFactory;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import org.apache.rocketmq.apis.consumer.ConsumeResult;
import org.apache.rocketmq.apis.consumer.MessageListener;
import org.apache.rocketmq.thinclient.hook.MessageHookPoints;
import org.apache.rocketmq.thinclient.hook.MessageHookPointsStatus;
import org.apache.rocketmq.thinclient.hook.MessageInterceptor;
import org.apache.rocketmq.thinclient.message.MessageCommon;
import org.apache.rocketmq.thinclient.message.MessageViewImpl;

public class ConsumeTask implements Callable<ConsumeResult> {
    private static final Logger LOGGER = LoggerFactory.getLogger(ConsumeTask.class);

    private final String clientId;
    private final MessageListener messageListener;
    private final MessageViewImpl messageView;
    private final MessageInterceptor messageInterceptor;

    public ConsumeTask(String clientId, MessageListener messageListener, MessageViewImpl messageView,
        MessageInterceptor messageInterceptor) {
        this.clientId = clientId;
        this.messageListener = messageListener;
        this.messageView = messageView;
        this.messageInterceptor = messageInterceptor;
    }

    /**
     * Invoke {@link MessageListener} to consumer message.
     *
     * @return message(s) which is consumed successfully.
     */
    @Override
    public ConsumeResult call() {
        ConsumeResult consumeResult;
        final List<MessageCommon> messageCommons = Collections.singletonList(messageView.getMessageCommon());
        messageInterceptor.doBefore(MessageHookPoints.CONSUME, messageCommons);
        final Stopwatch stopwatch = Stopwatch.createStarted();
        try {
            consumeResult = messageListener.consume(messageView);
        } catch (Throwable t) {
            LOGGER.error("Message listener raised an exception while consuming messages, client id={}", clientId, t);
            // If exception was thrown during the period of message consumption, mark it as failure.
            consumeResult = ConsumeResult.ERROR;
        }
        final Duration duration = stopwatch.elapsed();
        MessageHookPointsStatus status = ConsumeResult.OK.equals(consumeResult) ? MessageHookPointsStatus.OK : MessageHookPointsStatus.ERROR;
        messageInterceptor.doAfter(MessageHookPoints.CONSUME, messageCommons, duration, status);
        // Make sure that the return value is the subset of messageViews.
        return consumeResult;
    }
}
