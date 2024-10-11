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
package org.apache.rocketmq.controller.impl.closure;

import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.entity.Task;
import com.alipay.sofa.jraft.error.RaftError;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.rocketmq.controller.impl.event.ControllerResult;
import org.apache.rocketmq.remoting.CommandCustomHeader;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.ResponseCode;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.nio.ByteBuffer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class ControllerClosureTest {

    private ControllerClosure controllerClosure;

    @Mock
    private RemotingCommand requestEvent;

    @Mock
    private ControllerResult<Object> controllerResult;

    @Before
    public void init() throws IllegalAccessException {
        controllerClosure = new ControllerClosure(requestEvent);
        FieldUtils.writeDeclaredField(controllerClosure, "requestEvent", requestEvent, true);
        FieldUtils.writeDeclaredField(controllerClosure, "controllerResult", controllerResult, true);
    }

    @Test
    public void testRunStatusOk() {
        Status status = Status.OK();
        when(controllerResult.getResponseCode()).thenReturn(0);
        when(controllerResult.getResponse()).thenReturn(mock(CommandCustomHeader.class));
        controllerClosure.run(status);
        RemotingCommand response = controllerClosure.getFuture().join();
        assertEquals(0, response.getCode());
        verify(controllerResult).getResponseCode();
        verify(controllerResult).getResponse();
    }

    @Test
    public void testRunStatusOkWithBody() {
        Status status = Status.OK();
        byte[] body = "test body".getBytes();
        when(controllerResult.getBody()).thenReturn(body);
        controllerClosure.run(status);
        RemotingCommand response = controllerClosure.getFuture().join();
        assertEquals(body, response.getBody());
        verify(controllerResult, times(2)).getBody();
    }

    @Test
    public void testRunStatusOkWithRemark() {
        Status status = Status.OK();
        String remark = "test remark";
        when(controllerResult.getRemark()).thenReturn(remark);
        controllerClosure.run(status);
        RemotingCommand response = controllerClosure.getFuture().join();
        assertEquals(remark, response.getRemark());
        verify(controllerResult, times(2)).getRemark();
    }

    @Test
    public void testRunStatusNotOk() {
        Status status = new Status(RaftError.EINTERNAL, "internal error");
        controllerClosure.run(status);
        RemotingCommand response = controllerClosure.getFuture().join();
        assertEquals(ResponseCode.CONTROLLER_JRAFT_INTERNAL_ERROR, response.getCode());
        assertEquals("internal error", response.getRemark());
    }

    @Test
    public void testTaskWithThisClosureTaskIsNull() {
        when(requestEvent.encode()).thenReturn(ByteBuffer.allocate(4));
        Task task = controllerClosure.taskWithThisClosure();
        assertEquals(requestEvent.encode(), task.getData());
        assertNotNull(task);
        assertEquals(controllerClosure, task.getDone());
    }

    @Test
    public void testTaskWithThisClosureTaskIsNotNull() throws IllegalAccessException {
        Task expected = mock(Task.class);
        FieldUtils.writeDeclaredField(controllerClosure, "task", expected, true);
        Task actual = controllerClosure.taskWithThisClosure();
        assertEquals(expected, actual);
    }
}
