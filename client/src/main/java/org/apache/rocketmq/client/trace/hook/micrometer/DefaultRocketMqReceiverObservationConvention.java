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

import io.micrometer.common.KeyValues;
import org.apache.rocketmq.client.trace.hook.micrometer.RocketMqObservationDocumentation.HighCardinalityTags;

/**
 * Default implementation of {@link RocketMqReceiverObservationConvention}.
 */
public class DefaultRocketMqReceiverObservationConvention extends AbstractRocketMqObservationConvention implements RocketMqReceiverObservationConvention {

    /**
     * Singleton instance of this convention.
     */
    public static final DefaultRocketMqReceiverObservationConvention INSTANCE = new DefaultRocketMqReceiverObservationConvention();

    @Override
    public KeyValues getLowCardinalityKeyValues(RocketMqReceiverContext context) {
        return getLowCardinalityKeyValues("receive");
    }

    @Override
    public KeyValues getHighCardinalityKeyValues(RocketMqReceiverContext context) {
        // TODO: Is this the proper way to get the client group?
        return KeyValues.of(HighCardinalityTags.MESSAGING_ROCKETMQ_CLIENT_GROUP.withValue(context.getConsumeMessageContext().getConsumerGroup()),
                        // TODO: What is this consumption model?
                        HighCardinalityTags.MESSAGING_ROCKETMQ_CONSUMPTION_MODEL.withValue(""))
                // TODO: How do we set the producer group for consuming? Does it make any sense?
                // TODO: How do we get the host properly?
                .and(getHighCardinalityKeyValues(context.getCarrier(), null, context.getCarrier().getBornHost().toString(), context.getConsumeMessageContext().getNamespace()));
    }

    @Override
    public String getName() {
        return "rocketmq.receive";
    }

    @Override
    public String getContextualName(RocketMqReceiverContext context) {
        return "receive " + context.getConsumeMessageContext().getMq().getTopic();
    }
}
