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
package org.apache.rocketmq.snode.processor;
import java.util.concurrent.CompletableFuture;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.remoting.RemotingChannel;
import org.apache.rocketmq.remoting.RequestProcessor;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.snode.SnodeController;
import org.apache.rocketmq.snode.interceptor.ExceptionContext;
import org.apache.rocketmq.snode.interceptor.RequestContext;
import org.apache.rocketmq.snode.interceptor.ResponseContext;

public class SendMessageProcessor implements RequestProcessor {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.SNODE_LOGGER_NAME);

    private final SnodeController snodeController;

    public SendMessageProcessor(final SnodeController snodeController) {
        this.snodeController = snodeController;
    }

    @Override
    public RemotingCommand processRequest(RemotingChannel remotingChannel, RemotingCommand request) {
        if (this.snodeController.getSendMessageInterceptorGroup() != null) {
            RequestContext requestContext = new RequestContext(request, remotingChannel);
            this.snodeController.getSendMessageInterceptorGroup().beforeRequest(requestContext);
        }
        CompletableFuture<RemotingCommand> responseFuture = snodeController.getEnodeService().sendMessage(request);
        responseFuture.whenComplete((data, ex) -> {
            if (ex == null) {
                if (this.snodeController.getSendMessageInterceptorGroup() != null) {
                    ResponseContext responseContext = new ResponseContext(request, remotingChannel, data);
                    this.snodeController.getSendMessageInterceptorGroup().afterRequest(responseContext);
                }
                remotingChannel.reply(data);
            } else {
                if (this.snodeController.getSendMessageInterceptorGroup() != null) {
                    ExceptionContext exceptionContext = new ExceptionContext(request, remotingChannel, ex, null);
                    this.snodeController.getSendMessageInterceptorGroup().onException(exceptionContext);
                }
                log.error("Send Message error: {}", ex);
            }
        });
        return null;
    }

    @Override
    public boolean rejectRequest() {
        return false;
    }
}
