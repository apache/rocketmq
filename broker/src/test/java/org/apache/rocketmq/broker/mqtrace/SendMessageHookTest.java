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

package org.apache.rocketmq.broker.mqtrace;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import org.junit.Assert;
import org.junit.Test;

public class SendMessageHookTest {

    @Test
    public void order() {

        SendMessageHook hook1 = new SendMessageHook() {

            @Override
            public int order() {
                return 1;
            }

            @Override
            public String hookName() {
                return "hook1";
            }

            @Override
            public void sendMessageBefore(SendMessageContext context) {

            }

            @Override
            public void sendMessageAfter(SendMessageContext context) {

            }
        };
        SendMessageHook hook2 = new SendMessageHook() {

            @Override
            public int order() {
                return 2;
            }

            @Override
            public String hookName() {
                return "hook2";
            }

            @Override
            public void sendMessageBefore(SendMessageContext context) {

            }

            @Override
            public void sendMessageAfter(SendMessageContext context) {

            }
        };
        SendMessageHook hook3 = new SendMessageHook() {

            @Override
            public int order() {
                return 3;
            }

            @Override
            public String hookName() {
                return "hook3";
            }

            @Override
            public void sendMessageBefore(SendMessageContext context) {

            }

            @Override
            public void sendMessageAfter(SendMessageContext context) {

            }
        };

        List<SendMessageHook> list = new ArrayList<>();
        list.add(hook3);
        list.add(hook1);
        list.sort(Comparator.comparing(SendMessageHook::order));
        SendMessageHook hook = list.get(0);
        Assert.assertEquals("hook1", hook.hookName());
        list.add(hook2);
        hook = list.get(2);
        Assert.assertEquals("hook2", hook.hookName());
        list.sort(Comparator.comparing(SendMessageHook::order));
        hook = list.get(2);
        Assert.assertEquals("hook3", hook.hookName());

    }
}