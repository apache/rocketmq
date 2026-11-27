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
package org.apache.rocketmq.controller.impl;

import io.openmessaging.storage.dledger.AppendFuture;
import io.openmessaging.storage.dledger.DLedgerConfig;
import io.openmessaging.storage.dledger.DLedgerServer;
import io.openmessaging.storage.dledger.protocol.AppendEntryRequest;
import io.openmessaging.storage.dledger.protocol.AppendEntryResponse;
import io.openmessaging.storage.dledger.protocol.DLedgerResponseCode;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import org.apache.rocketmq.common.ControllerConfig;
import org.apache.rocketmq.controller.impl.event.ControllerResult;
import org.apache.rocketmq.controller.impl.event.EventSerializer;
import org.apache.rocketmq.controller.impl.event.RegisterBrokerStartupEvent;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.ResponseCode;
import org.apache.rocketmq.remoting.protocol.header.controller.register.RegisterBrokerToControllerResponseHeader;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DLedgerControllerAppendTest {
    private DLedgerController controller;
    private DLedgerServer server;

    @Before
    public void setUp() throws Exception {
        // Exercise the real append/response code without starting network or storage services.
        controller = mock(DLedgerController.class, CALLS_REAL_METHODS);
        server = mock(DLedgerServer.class);
        setField("dLedgerServer", server);
        DLedgerConfig ledgerConfig = new DLedgerConfig();
        ledgerConfig.setGroup("startup");
        ledgerConfig.setSelfId("n0");
        setField("dLedgerConfig", ledgerConfig);
        setField("controllerConfig", new ControllerConfig());
        setField("eventSerializer", new EventSerializer());
    }

    @Test
    public void completedErrorResponseIsNotACommittedGrant() throws Exception {
        assertFalse(append(DLedgerResponseCode.WAIT_QUORUM_ACK_TIMEOUT));
        assertFalse(append(DLedgerResponseCode.TERM_CHANGED));
        assertTrue(append(DLedgerResponseCode.SUCCESS));
    }

    @Test
    public void registrationReturnsCommittedEpochInsteadOfPredictedEpoch() throws Throwable {
        ControllerResult<RegisterBrokerToControllerResponseHeader> predicted = result(2);
        predicted.addEvent(new RegisterBrokerStartupEvent("broker", 1L, "127.0.0.1:10911", 3, "third"));
        ControllerResult<RegisterBrokerToControllerResponseHeader> committed = result(3);
        completeAppend(DLedgerResponseCode.SUCCESS);
        DLedgerController.ControllerEventHandler<RegisterBrokerToControllerResponseHeader> handler =
            controller.new ControllerEventHandler<>("register", () -> predicted, true, () -> committed);
        handler.run();
        RemotingCommand response = handler.future().get();
        assertEquals(ResponseCode.SUCCESS, response.getCode());
        RegisterBrokerToControllerResponseHeader header =
            (RegisterBrokerToControllerResponseHeader) response.readCustomHeader();
        assertEquals(Integer.valueOf(3), header.getMasterEpoch());
    }

    @Test
    public void registrationRevalidatesIdentityAfterCommit() throws Throwable {
        ControllerResult<RegisterBrokerToControllerResponseHeader> predicted = result(2);
        predicted.addEvent(new RegisterBrokerStartupEvent("broker", 1L, "127.0.0.1:10911", 2, "second"));
        ControllerResult<RegisterBrokerToControllerResponseHeader> superseded = result(3);
        superseded.setCodeAndRemark(ResponseCode.CONTROLLER_INVALID_REQUEST, "Startup was superseded");
        completeAppend(DLedgerResponseCode.SUCCESS);
        DLedgerController.ControllerEventHandler<RegisterBrokerToControllerResponseHeader> handler =
            controller.new ControllerEventHandler<>("register", () -> predicted, true, () -> superseded);
        handler.run();
        assertEquals(ResponseCode.CONTROLLER_INVALID_REQUEST, handler.future().get().getCode());
    }

    @Test(expected = IllegalStateException.class)
    public void unappliedStartupCannotProduceSuccess() throws Throwable {
        ControllerResult<RegisterBrokerToControllerResponseHeader> unapplied = result(2);
        unapplied.addEvent(new RegisterBrokerStartupEvent("broker", 1L, "127.0.0.1:10911", 2, "second"));
        completeAppend(DLedgerResponseCode.SUCCESS);
        controller.new ControllerEventHandler<>("register", () -> unapplied, true, () -> unapplied).run();
    }

    private ControllerResult<RegisterBrokerToControllerResponseHeader> result(int epoch) {
        RegisterBrokerToControllerResponseHeader header = new RegisterBrokerToControllerResponseHeader();
        header.setMasterEpoch(epoch);
        return new ControllerResult<>(header);
    }

    private boolean append(DLedgerResponseCode code) throws Exception {
        completeAppend(code);
        Method method = DLedgerController.class.getDeclaredMethod("appendToDLedgerAndWait", AppendEntryRequest.class);
        method.setAccessible(true);
        return (boolean) method.invoke(controller, new AppendEntryRequest());
    }

    private void completeAppend(DLedgerResponseCode code) throws Exception {
        AppendEntryResponse response = new AppendEntryResponse();
        response.setCode(code.getCode());
        AppendFuture<AppendEntryResponse> future = new AppendFuture<>();
        future.setPos(1);
        future.complete(response);
        when(server.handleAppend(any(AppendEntryRequest.class))).thenReturn(future);
    }

    private void setField(String name, Object value) throws Exception {
        Field field = DLedgerController.class.getDeclaredField(name);
        field.setAccessible(true);
        field.set(controller, value);
    }
}
