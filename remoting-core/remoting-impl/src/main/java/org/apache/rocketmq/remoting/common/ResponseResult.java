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

package org.apache.rocketmq.remoting.common;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.apache.rocketmq.remoting.api.AsyncHandler;
import org.apache.rocketmq.remoting.api.RemotingEndPoint;
import org.apache.rocketmq.remoting.api.command.RemotingCommand;
import org.apache.rocketmq.remoting.api.interceptor.ExceptionContext;
import org.apache.rocketmq.remoting.api.interceptor.InterceptorGroup;
import org.apache.rocketmq.remoting.api.interceptor.ResponseContext;

public class ResponseResult {
    private final long beginTimestamp = System.currentTimeMillis();
    private final CountDownLatch countDownLatch = new CountDownLatch(1);
    private final AtomicBoolean interceptorExecuted = new AtomicBoolean(false);

    private int requestId;
    private long timeoutMillis;
    private AsyncHandler asyncHandler;

    private volatile RemotingCommand responseCommand;
    private volatile boolean sendRequestOK = true;
    private volatile Throwable cause;
    private SemaphoreReleaseOnlyOnce once;

    private RemotingCommand requestCommand;
    private InterceptorGroup interceptorGroup;
    private String remoteAddr;

    public ResponseResult(int requestId, long timeoutMillis, AsyncHandler asyncHandler, SemaphoreReleaseOnlyOnce once) {
        this.requestId = requestId;
        this.timeoutMillis = timeoutMillis;
        this.asyncHandler = asyncHandler;
        this.once = once;
    }

    public ResponseResult(int requestId, long timeoutMillis) {
        this.requestId = requestId;
        this.timeoutMillis = timeoutMillis;
    }

    public void executeRequestSendFailed() {
        if (this.interceptorExecuted.compareAndSet(false, true)) {
            try {
                interceptorGroup.onException(new ExceptionContext(RemotingEndPoint.REQUEST, this.remoteAddr, this.requestCommand,
                    cause, "REQUEST_SEND_FAILED"));
            } catch (Throwable e) {
            }
            //Sync call
            if (null != asyncHandler) {
                asyncHandler.onFailure(requestCommand);
            }
        }
    }

    public void executeCallbackArrived(final RemotingCommand response) {
        if (this.interceptorExecuted.compareAndSet(false, true)) {
            try {
                interceptorGroup.afterResponseReceived(new ResponseContext(RemotingEndPoint.REQUEST, this.remoteAddr,
                    this.requestCommand, response));
            } catch (Throwable e) {
            }
            if (null != asyncHandler) {
                asyncHandler.onSuccess(response);
            }
        }
    }

    public void onTimeout(long costTimeMillis, long timoutMillis) {
        if (this.interceptorExecuted.compareAndSet(false, true)) {
            try {
                interceptorGroup.onException(new ExceptionContext(RemotingEndPoint.REQUEST, this.remoteAddr, this.requestCommand,
                    null, "CALLBACK_TIMEOUT"));
            } catch (Throwable e) {
            }
            if (null != asyncHandler) {
                asyncHandler.onTimeout(costTimeMillis, timoutMillis);
            }
        }
    }

    public void release() {
        if (this.once != null) {
            this.once.release();
        }
    }

    public RemotingCommand waitResponse(final long timeoutMillis) {
        try {
            this.countDownLatch.await(timeoutMillis, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return this.responseCommand;
    }

    public void putResponse(final RemotingCommand responseCommand) {
        this.responseCommand = responseCommand;
        this.countDownLatch.countDown();
    }

    public long getBeginTimestamp() {
        return beginTimestamp;
    }

    public boolean isSendRequestOK() {
        return sendRequestOK;
    }

    public void setSendRequestOK(boolean sendRequestOK) {
        this.sendRequestOK = sendRequestOK;
    }

    public long getTimeoutMillis() {
        return timeoutMillis;
    }

    public AsyncHandler getAsyncHandler() {
        return asyncHandler;
    }

    public Throwable getCause() {
        return cause;
    }

    public void setCause(Throwable cause) {
        this.cause = cause;
    }

    public RemotingCommand getResponseCommand() {
        return responseCommand;
    }

    public void setResponseCommand(RemotingCommand responseCommand) {
        this.responseCommand = responseCommand;
    }

    public int getRequestId() {
        return requestId;
    }

    public RemotingCommand getRequestCommand() {
        return requestCommand;
    }

    public void setRequestCommand(RemotingCommand requestCommand) {
        this.requestCommand = requestCommand;
    }

    public InterceptorGroup getInterceptorGroup() {
        return interceptorGroup;
    }

    public void setInterceptorGroup(InterceptorGroup interceptorGroup) {
        this.interceptorGroup = interceptorGroup;
    }

    public String getRemoteAddr() {
        return remoteAddr;
    }

    public void setRemoteAddr(String remoteAddr) {
        this.remoteAddr = remoteAddr;
    }

    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this, ToStringStyle.MULTI_LINE_STYLE);
    }
}
