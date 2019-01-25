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
package org.apache.rocketmq.snode.flowcontrol;

import java.util.concurrent.atomic.AtomicLong;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.flowcontrol.AbstractFlowControlService;
import org.apache.rocketmq.common.protocol.RequestCode;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.remoting.exception.RemotingRuntimeException;
import org.apache.rocketmq.remoting.interceptor.RequestContext;
import org.apache.rocketmq.remoting.protocol.RemotingSysResponseCode;

public class QPSFlowControlServiceImpl extends AbstractFlowControlService {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.SNODE_LOGGER_NAME);

    private final AtomicLong logCount = new AtomicLong(0);

    private final String flowControlType = "countLimit";

    public QPSFlowControlServiceImpl() {
        super();
    }

    @Override
    public String getResourceName(RequestContext requestContext) {
        if (RequestCode.HEART_BEAT == requestContext.getRequest().getCode()) {
            return null;
        }
        return requestContext.getRequest().getCode() + "";
    }

    @Override
    public String getFlowControlType() {
        return this.flowControlType;
    }

    @Override
    public void rejectRequest(RequestContext requestContext) {
        if (logCount.getAndIncrement() % 100 == 0) {
            log.warn("[REJECT]exceed system flow control config QPS, start flow control for a while: requestContext: {} ", requestContext);
        }
        throw new RemotingRuntimeException(RemotingSysResponseCode.SYSTEM_BUSY, "[REJECT]exceed system flow control config QPS, start flow control for a while");
    }

    @Override
    public String interceptorName() {
        return "snodeQPSFlowControlInterceptor";
    }

    @Override
    public int getResourceCount(RequestContext requestContext) {
        return 1;
    }
}
