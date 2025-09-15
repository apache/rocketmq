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
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.verify;

import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.store.MessageStoreStateMachine.MessageStoreState;
import org.junit.Test;
import org.junit.Before;
import org.mockito.Mockito;

public class MessageStoreStateMachineTest {

    private Logger mockLogger;
    private MessageStoreStateMachine stateMachine;

    @Before
    public void setUp() {
        // Mock Logger
        mockLogger = Mockito.mock(Logger.class);

        // Initialize StateMachine
        stateMachine = new MessageStoreStateMachine(mockLogger);
    }

    /**
     * Test the constructor of MessageStoreStateMachine.
     */
    @Test
    public void testConstructor() {
        // Verify initial state
        assertEquals(MessageStoreState.INIT, stateMachine.getCurrentState());

        // Verify logger was called for initialization
        verify(mockLogger).info(anyString(), eq(MessageStoreState.INIT));
    }

    /**
     * Test valid state transition in transitTo method.
     */
    @Test
    public void testValidStateTransition() {
        // Perform a valid state transition
        stateMachine.transitTo(MessageStoreState.LOAD_COMMITLOG_OK);

        // Verify the current state is updated
        assertEquals(MessageStoreState.LOAD_COMMITLOG_OK, stateMachine.getCurrentState());

        // Verify logger was called for state transition
        verify(mockLogger).info(anyString(), eq(MessageStoreState.INIT), eq(MessageStoreState.LOAD_COMMITLOG_OK),
            anyLong(), anyLong());
    }

    /**
     * Test fail state transition in transitTo method.
     */
    @Test
    public void testValidFailStateTransition() {
        stateMachine.transitTo(MessageStoreState.LOAD_COMMITLOG_OK, false);
        assertEquals(MessageStoreState.INIT, stateMachine.getCurrentState());
        verify(mockLogger).warn(anyString(), eq(MessageStoreState.INIT), eq(MessageStoreState.LOAD_COMMITLOG_OK),
            anyLong(), anyLong());
    }

    /**
     * Test invalid state transition in transitTo method.
     */
    @Test
    public void testInvalidStateTransition() {
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
    public void testGetCurrentState() {
        // Verify the current state
        assertEquals(MessageStoreState.INIT, stateMachine.getCurrentState());
    }
}
