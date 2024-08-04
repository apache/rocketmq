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
package org.apache.rocketmq.common.chain;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class HandlerChainTest {

    private HandlerChain<Integer, String> handlerChain;
    private Handler<Integer, String> handler1;
    private Handler<Integer, String> handler2;

    @Before
    public void setUp() {
        handlerChain = HandlerChain.create();
        handler1 = (t, chain) -> "Handler1";
        handler2 = (t, chain) -> null;
    }

    @Test
    public void testHandle_withEmptyChain() {
        handlerChain.addNext(handler1);
        handlerChain.handle(1);
        assertNull("Expected null since the handler chain is empty", handlerChain.handle(2));
    }

    @Test
    public void testHandle_withNonEmptyChain() {
        handlerChain.addNext(handler1);

        String result = handlerChain.handle(1);

        assertEquals("Handler1", result);
    }

    @Test
    public void testHandle_withMultipleHandlers() {
        handlerChain.addNext(handler1);
        handlerChain.addNext(handler2);

        String result1 = handlerChain.handle(1);
        String result2 = handlerChain.handle(2);

        assertEquals("Handler1", result1);
        assertNull("Expected null since there are no more handlers", result2);
    }
}
