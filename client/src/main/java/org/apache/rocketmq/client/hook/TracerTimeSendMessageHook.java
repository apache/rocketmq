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

package org.apache.rocketmq.client.hook;

import java.lang.reflect.Method;
import org.apache.rocketmq.common.ClientTracerTimeUtil;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageConst;

public class TracerTimeSendMessageHook implements SendMessageHook {

    @Override
    public String hookName() {
        return "TracerTimeSendMessageHook";
    }

    @Override
    public void sendMessageBefore(SendMessageContext context) {
        tracerTimeIfNecessary(context.getMessage(), MessageConst.MESSAGE_SEND_TIME);
    }

    @Override
    public void sendMessageAfter(SendMessageContext context) {
        tracerTimeIfNecessary(context.getMessage(), MessageConst.RECEIVE_SEND_ACK_TIME);
    }

    public void tracerTimeIfNecessary(Message msg, String propertyKey) {
        if (ClientTracerTimeUtil.isEnableTracerTime()) {
            try {
                Method putPropertyMethod = msg.getClass().getDeclaredMethod("putProperty", String.class, String.class);
                putPropertyMethod.setAccessible(true);
                putPropertyMethod.invoke(msg, propertyKey, String.valueOf(System.currentTimeMillis()));
            } catch (Exception e) {
            }
        }
    }
}
