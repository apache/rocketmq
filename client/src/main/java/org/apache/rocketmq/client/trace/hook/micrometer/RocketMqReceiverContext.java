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
package org.apache.rocketmq.client.trace.hook.micrometer;

import io.micrometer.common.lang.NonNull;
import io.micrometer.observation.transport.ReceiverContext;
import org.apache.rocketmq.client.hook.ConsumeMessageContext;
import org.apache.rocketmq.common.message.MessageExt;

/**
 * A {@link ReceiverContext} for RocketMQ messages.
 */
public class RocketMqReceiverContext extends ReceiverContext<MessageExt> {

    private final ConsumeMessageContext consumeMessageContext;

    public RocketMqReceiverContext(@NonNull ConsumeMessageContext consumeMessageContext, MessageExt messageExt) {
        super((ctx, s) -> ctx.getProperties().get(s));
        this.consumeMessageContext = consumeMessageContext;
        setCarrier(messageExt);
    }

    public ConsumeMessageContext getConsumeMessageContext() {
        return consumeMessageContext;
    }
}
