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

import io.micrometer.common.lang.Nullable;
import io.micrometer.observation.Observation;
import io.micrometer.observation.Observation.Scope;
import io.micrometer.observation.ObservationRegistry;
import org.apache.rocketmq.client.hook.SendMessageContext;
import org.apache.rocketmq.client.hook.SendMessageHook;

/**
 * A {@link SendMessageHook} for Micrometer Observation.
 */
public class SendMessageMicrometerHookImpl implements SendMessageHook {

    private final ObservationRegistry observationRegistry;

    private final RocketMqSenderObservationConvention convention;

    public SendMessageMicrometerHookImpl(ObservationRegistry observationRegistry, @Nullable RocketMqSenderObservationConvention convention) {
        this.observationRegistry = observationRegistry;
        this.convention = convention;
    }

    @Override
    public String hookName() {
        return "SendMessageMicrometerObservationHook";
    }

    @Override
    public void sendMessageBefore(SendMessageContext context) {
        if (context == null) {
            return;
        }
        RocketMqSenderContext senderContext = new RocketMqSenderContext(context);
        Observation observation = RocketMqObservationDocumentation.MESSAGE_OUT.start(this.convention, DefaultRocketMqSenderObservationConvention.INSTANCE, () -> senderContext, observationRegistry);
        context.setMqTraceContext(observation.openScope());
    }

    @Override
    public void sendMessageAfter(SendMessageContext context) {
        if (context == null || context.getMqTraceContext() == null) {
            return;
        }
        if (context.getSendResult() == null) {
            return;
        }

        if (context.getSendResult().getRegionId() == null) {
            return;
        }
        Observation.Scope scope = (Scope) context.getMqTraceContext();
        Observation observation = scope.getCurrentObservation();
        scope.close();
        observation.stop();
    }
}
