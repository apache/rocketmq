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
package org.apache.rocketmq.broker.util;

import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageExtBrokerInner;
import org.apache.rocketmq.store.PutMessageResult;
import org.apache.rocketmq.store.PutMessageStatus;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

public class HookUtilsTimerOverflowTest {

    private BrokerController newTimerEnabledController() {
        BrokerController brokerController = Mockito.mock(BrokerController.class);
        MessageStoreConfig messageStoreConfig = new MessageStoreConfig();
        messageStoreConfig.setTimerWheelEnable(true);
        Mockito.when(brokerController.getMessageStoreConfig()).thenReturn(messageStoreConfig);
        return brokerController;
    }

    private MessageExtBrokerInner newTimerMessage(String key, String value) {
        MessageExtBrokerInner msg = new MessageExtBrokerInner();
        msg.setTopic("OverflowTestTopic");
        MessageAccessor.putProperty(msg, key, value);
        return msg;
    }

    // Regression for https://github.com/apache/rocketmq/issues/10872: an
    // overflowing relative delay used to wrap around into a past timestamp,
    // bypass the future-delay validation, and take the immediate-message path.
    @Test
    public void testTimerDelaySecOverflowRejected() {
        PutMessageResult result = HookUtils.handleScheduleMessage(newTimerEnabledController(),
            newTimerMessage(MessageConst.PROPERTY_TIMER_DELAY_SEC, String.valueOf(Long.MAX_VALUE)));
        Assert.assertNotNull(result);
        Assert.assertEquals(PutMessageStatus.WHEEL_TIMER_MSG_ILLEGAL, result.getPutMessageStatus());
    }

    @Test
    public void testTimerDelayMsOverflowRejected() {
        PutMessageResult result = HookUtils.handleScheduleMessage(newTimerEnabledController(),
            newTimerMessage(MessageConst.PROPERTY_TIMER_DELAY_MS, String.valueOf(Long.MAX_VALUE)));
        Assert.assertNotNull(result);
        Assert.assertEquals(PutMessageStatus.WHEEL_TIMER_MSG_ILLEGAL, result.getPutMessageStatus());
    }

    // Covers the future-delivery validation branch with a delay that does not
    // overflow but exceeds the configured maximum.
    @Test
    public void testTimerDelaySecExceedsMaxRejected() {
        BrokerController brokerController = newTimerEnabledController();
        brokerController.getMessageStoreConfig().setTimerMaxDelaySec(1);
        PutMessageResult result = HookUtils.handleScheduleMessage(brokerController,
            newTimerMessage(MessageConst.PROPERTY_TIMER_DELAY_SEC, "10"));
        Assert.assertNotNull(result);
        Assert.assertEquals(PutMessageStatus.WHEEL_TIMER_MSG_ILLEGAL, result.getPutMessageStatus());
    }
}
