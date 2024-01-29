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

import com.alipay.sofa.jraft.Closure;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.entity.Task;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.controller.impl.event.ControllerResult;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.CommandCustomHeader;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.ResponseCode;

import java.util.concurrent.CompletableFuture;

public class ControllerClosure implements Closure {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.CONTROLLER_LOGGER_NAME);
    private final RemotingCommand requestEvent;
    private final CompletableFuture<RemotingCommand> future;
    private ControllerResult<?> controllerResult;
    private Task task;

    public ControllerClosure(RemotingCommand requestEvent) {
        this.requestEvent = requestEvent;
        this.future = new CompletableFuture<>();
        this.task = null;
    }

    public CompletableFuture<RemotingCommand> getFuture() {
        return future;
    }

    public void setControllerResult(ControllerResult<?> controllerResult) {
        this.controllerResult = controllerResult;
    }

    @Override
    public void run(Status status) {
        if (status.isOk()) {
            final RemotingCommand response = RemotingCommand.createResponseCommandWithHeader(controllerResult.getResponseCode(), (CommandCustomHeader) controllerResult.getResponse());
            if (controllerResult.getBody() != null) {
                response.setBody(controllerResult.getBody());
            }
            if (controllerResult.getRemark() != null) {
                response.setRemark(controllerResult.getRemark());
            }
            future.complete(response);
        } else {
            log.error("Failed to append to jRaft node, error is: {}.", status);
            future.complete(RemotingCommand.createResponseCommand(ResponseCode.CONTROLLER_JRAFT_INTERNAL_ERROR, status.getErrorMsg()));
        }
    }

    public Task taskWithThisClosure() {
        if (task != null) {
            return task;
        }
        task = new Task();
        task.setDone(this);
        task.setData(requestEvent.encode());
        return task;
    }

    public RemotingCommand getRequestEvent() {
        return requestEvent;
    }
}
