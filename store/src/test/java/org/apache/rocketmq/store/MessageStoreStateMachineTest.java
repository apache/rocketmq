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

package org.apache.rocketmq.store;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.verify;

import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.store.MessageStoreStateMachine.MessageStoreState;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class MessageStoreStateMachineTest {

    private Logger mockLogger;
    private MessageStoreStateMachine stateMachine;

    @BeforeEach
    void setUp() {
        // Mock Logger
        mockLogger = Mockito.mock(Logger.class);

        // Initialize StateMachine
        stateMachine = new MessageStoreStateMachine(mockLogger);
    }

    /**
     * Test the constructor of MessageStoreStateMachine.
     */
    @Test
    void testConstructor() {
        // Verify initial state
        assertEquals(MessageStoreState.INIT, stateMachine.getCurrentState());

        // Verify logger was called for initialization
        verify(mockLogger).info("MessageStore initialized, state={}", MessageStoreState.INIT);
    }

    /**
     * Test valid state transition in transitTo method.
     */
    @Test
    void testValidStateTransition() {
        // Perform a valid state transition
        stateMachine.transitTo(MessageStoreState.LOAD_COMMITLOG_OK);

        // Verify the current state is updated
        assertEquals(MessageStoreState.LOAD_COMMITLOG_OK, stateMachine.getCurrentState());

        // Verify logger was called for state transition
        verify(mockLogger).info(
            eq("MessageStore state transition from {} to {}; Time in previous state: {} ms, Total time: {} ms"),
            eq(MessageStoreState.INIT), eq(MessageStoreState.LOAD_COMMITLOG_OK), anyLong(), anyLong()
        );
    }

    /**
     * Test invalid state transition in transitTo method.
     */
    @Test
    void testInvalidStateTransition() {
        // Perform an invalid state transition
        Exception exception = assertThrows(IllegalStateException.class, () -> {
            stateMachine.transitTo(MessageStoreState.INIT);
        });

        // Verify the exception message
        String expectedMessage = "Invalid state transition from INIT to INIT. Can only move forward.";
        assertEquals(expectedMessage, exception.getMessage());
    }

    /**
     * Test getCurrentState method.
     */
    @Test
    void testGetCurrentState() {
        // Verify the current state
        assertEquals(MessageStoreState.INIT, stateMachine.getCurrentState());
    }

    /**
     * Test getTotalRunningTimeMs method.
     */
    @Test
    void testGetTotalRunningTimeMs() {
        // Sleep for a short duration to simulate elapsed time
        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        // Verify the total running time is approximately correct
        long totalTime = stateMachine.getTotalRunningTimeMs();
        assertTrue(totalTime >= 100 && totalTime < 200);
    }

    /**
     * Test getCurrentStateRunningTimeMs method.
     */
    @Test
    void testGetCurrentStateRunningTimeMs() {
        // Perform a state transition
        stateMachine.transitTo(MessageStoreState.LOAD_COMMITLOG_OK);

        // Sleep for a short duration to simulate elapsed time
        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        // Verify the current state running time is approximately correct
        long currentStateTime = stateMachine.getCurrentStateRunningTimeMs();
        assertTrue(currentStateTime >= 100 && currentStateTime < 200);
    }
}
