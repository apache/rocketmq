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

import java.util.ArrayList;
import java.util.List;

import io.micrometer.common.lang.Nullable;
import io.micrometer.observation.Observation;
import io.micrometer.observation.ObservationRegistry;
import org.apache.rocketmq.client.hook.ConsumeMessageContext;
import org.apache.rocketmq.client.hook.ConsumeMessageHook;
import org.apache.rocketmq.common.message.MessageExt;

/**
 * A {@link ConsumeMessageHook} for Micrometer Observation.
 */
public class ConsumeMessageMicrometerHookImpl implements ConsumeMessageHook {

    private final ObservationRegistry observationRegistry;

    private final RocketMqReceiverObservationConvention convention;

    public ConsumeMessageMicrometerHookImpl(ObservationRegistry observationRegistry, @Nullable RocketMqReceiverObservationConvention convention) {
        this.observationRegistry = observationRegistry;
        this.convention = convention;
    }

    @Override
    public String hookName() {
        return "ConsumeMessageOpenTracingHook";
    }

    @Override
    public void consumeMessageBefore(ConsumeMessageContext context) {
        if (context == null || context.getMsgList() == null || context.getMsgList().isEmpty()) {
            return;
        }
        List<Observation> observationList = new ArrayList<>();
        // TODO: What about scoping? Do I always consume messages in batches? How often will I process just 1 message ext ?
        for (MessageExt msg : context.getMsgList()) {
            if (msg == null) {
                continue;
            }
            RocketMqReceiverContext receiverContext = new RocketMqReceiverContext(context, msg);
            Observation observation = RocketMqObservationDocumentation.MESSAGE_IN.start(convention, DefaultRocketMqReceiverObservationConvention.INSTANCE, () -> receiverContext, observationRegistry);
            observationList.add(observation);
        }
        context.setMqTraceContext(observationList);
    }

    @Override
    public void consumeMessageAfter(ConsumeMessageContext context) {
        if (context == null || context.getMsgList() == null || context.getMsgList().isEmpty()) {
            return;
        }
        List<Observation> observations = (List<Observation>) context.getMqTraceContext();
        if (observations == null) {
            return;
        }
        for (Observation observation : observations) {
            observation.stop();
        }
    }
}
