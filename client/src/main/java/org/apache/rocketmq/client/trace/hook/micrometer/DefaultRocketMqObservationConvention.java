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
import io.micrometer.common.util.StringUtils;
import org.apache.rocketmq.client.trace.hook.micrometer.RocketMqObservationDocumentation.HighCardinalityTags;
import org.apache.rocketmq.client.trace.hook.micrometer.RocketMqObservationDocumentation.LowCardinalityTags;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageConst;

public class DefaultRocketMqObservationConvention extends AbstractRocketMqObservationConvention implements RocketMqSenderObservationConvention {

	@Override
	public KeyValues getLowCardinalityKeyValues(RocketMqSenderContext context) {
		return KeyValues.of(
				LowCardinalityTags.MESSAGING_ROCKETMQ_MESSAGE_TYPE.withValue(context.getSendMessageContext().getMsgType().getShortName())).and(getLowCardinalityKeyValues("publish"));
	}

	@Override
	public KeyValues getHighCardinalityKeyValues(RocketMqSenderContext context) {
		return getHighCardinalityKeyValues(context.getSendMessageContext().getMessage(), context.getSendMessageContext().getProducerGroup(), context.getSendMessageContext().getBrokerAddr());
	}

	@Override
	public String getName() {
		return "rocketmq.publish";
	}

	@Override
	public String getContextualName(RocketMqSenderContext context) {
		return "produce " + context.getSendMessageContext().getMessage().getTopic();
	}
}
