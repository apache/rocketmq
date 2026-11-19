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
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.controller.impl.event.ControllerResult;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.ResponseCode;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class ControllerClosureTest {

    private RemotingCommand newRequest() {
        return RemotingCommand.createRequestCommand(0, null);
    }

    @Test
    public void testOkStatusCompletesFutureWithCodeBodyAndRemark() throws Exception {
        ControllerClosure closure = new ControllerClosure(newRequest());

        ControllerResult<Object> result = new ControllerResult<>(null);
        result.setCodeAndRemark(ResponseCode.SUCCESS, "applied");
        result.setBody("payload".getBytes(StandardCharsets.UTF_8));
        closure.setControllerResult(result);

        closure.run(new Status());

        RemotingCommand response = closure.getFuture().get(5, TimeUnit.SECONDS);
        assertThat(response.getCode()).isEqualTo(ResponseCode.SUCCESS);
        assertThat(response.getRemark()).isEqualTo("applied");
        assertThat(new String(response.getBody(), StandardCharsets.UTF_8)).isEqualTo("payload");
    }

    @Test
    public void testOkStatusWithoutBodyAndRemarkLeavesThemUnset() throws Exception {
        ControllerClosure closure = new ControllerClosure(newRequest());

        ControllerResult<Object> result = new ControllerResult<>(null);
        closure.setControllerResult(result);

        closure.run(new Status());

        RemotingCommand response = closure.getFuture().get(5, TimeUnit.SECONDS);
        assertThat(response.getCode()).isEqualTo(ResponseCode.SUCCESS);
        assertThat(response.getBody()).isNull();
        assertThat(response.getRemark()).isNull();
    }

    @Test
    public void testFailedStatusMapsToJraftInternalError() throws Exception {
        ControllerClosure closure = new ControllerClosure(newRequest());

        closure.run(new Status(RaftError.UNKNOWN, "boom"));

        RemotingCommand response = closure.getFuture().get(5, TimeUnit.SECONDS);
        assertThat(response.getCode()).isEqualTo(ResponseCode.CONTROLLER_JRAFT_INTERNAL_ERROR);
        assertThat(response.getRemark()).isEqualTo("boom");
    }

    @Test
    public void testTaskWithThisClosureIsCachedAndCarriesEncodedRequest() {
        RemotingCommand request = newRequest();
        ControllerClosure closure = new ControllerClosure(request);

        Task first = closure.taskWithThisClosure();
        Task second = closure.taskWithThisClosure();

        assertThat(second).isSameAs(first);
        assertThat(first.getDone()).isSameAs(closure);
        assertThat(closure.getRequestEvent()).isSameAs(request);
        // the task data is the serialized request this closure was built from
        assertThat(first.getData()).isEqualTo(request.encode());
    }
}
