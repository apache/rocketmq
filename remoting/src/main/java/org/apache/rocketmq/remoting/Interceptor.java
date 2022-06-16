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
package org.apache.rocketmq.remoting;

import org.apache.rocketmq.remoting.protocol.RemotingCommand;

import java.util.concurrent.CompletableFuture;

/**
 * <strong>Thread Safety Requirement</strong>
 *
 * This interceptor will be invoked concurrently and its implementations are required to be thread-safe.
 */
public interface Interceptor {

    /**
     * Execute the first half of the interceptor when an incoming request is being sent(client-side) or received(server-side).
     *
     * @param context The interceptor context
     *
     * @param request The request
     *
     * @param responseFuture If the interceptor aborts further processing, use it to feed response.
     *
     * @return {@link Decision#CONTINUE} if the current interceptor passes OK and follow-up interceptors should execute as
     * normal; {@link Decision#STOP} if the request failed some preconditions. The interceptor implementation should generate
     * and feed a response command representing the failure into <code>responseFuture</code>.
     */
    Decision preHandle(final InterceptorContext context, final RemotingCommand request,
        final CompletableFuture<RemotingCommand> responseFuture);

    /**
     * Execute the post half of the interceptor when response is generated(server-side) or received(client-side).
     *
     * @param context The interceptor context.
     *
     * @param request The request
     *
     * @param response Response generated or received. Interceptors may modify it according to application requirement.
     *
     * @return {@link Decision#CONTINUE} if follow-up interceptors should execute; {@link Decision#STOP} otherwise.
     */
    Decision postHandle(final InterceptorContext context, final RemotingCommand request, final RemotingCommand response);

}
