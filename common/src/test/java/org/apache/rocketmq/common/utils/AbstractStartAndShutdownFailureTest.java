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

package org.apache.rocketmq.common.utils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;

public class AbstractStartAndShutdownFailureTest {
    @Test
    public void testShutdownContinuesInReverseOrderAndPreservesFailures() {
        assertAllTargetsInvoked(false);
    }

    @Test
    public void testPreShutdownContinuesInReverseOrderAndPreservesFailures() {
        assertAllTargetsInvoked(true);
    }

    private void assertAllTargetsInvoked(boolean preShutdown) {
        AbstractStartAndShutdown lifecycle = new AbstractStartAndShutdown() {
        };
        List<Integer> calls = new ArrayList<>();
        Exception firstFailure = new Exception("last registered target");
        Exception secondFailure = new Exception("first registered target");
        for (int index = 0; index < 3; index++) {
            final int targetIndex = index;
            lifecycle.appendStartAndShutdown(new StartAndShutdown() {
                @Override
                public void start() {
                }

                @Override
                public void shutdown() throws Exception {
                    invoke();
                }

                @Override
                public void preShutdown() throws Exception {
                    invoke();
                }

                private void invoke() throws Exception {
                    calls.add(targetIndex);
                    if (targetIndex == 2) {
                        throw firstFailure;
                    }
                    if (targetIndex == 0) {
                        throw secondFailure;
                    }
                }
            });
        }
        Exception reported = assertThrows(Exception.class, () -> {
            if (preShutdown) {
                lifecycle.preShutdown();
            } else {
                lifecycle.shutdown();
            }
        });
        assertEquals(Arrays.asList(2, 1, 0), calls);
        assertSame(firstFailure, reported);
        assertEquals(1, reported.getSuppressed().length);
        assertSame(secondFailure, reported.getSuppressed()[0]);
    }
}
