package org.apache.rocketmq.snode.interceptor;/*
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

import java.util.ArrayList;
import java.util.List;

public class InterceptorGroup {
    private List<Interceptor> interceptors = new ArrayList<>();

    public void registerInterceptor(Interceptor sendMessageInterceptor) {
        if (sendMessageInterceptor != null) {
            interceptors.add(sendMessageInterceptor);
        }
    }

    public void beforeRequest(RequestContext requestContext) {
        for (Interceptor interceptor : interceptors) {
            interceptor.beforeSendMessage(requestContext);
        }
    }

    public void afterRequest(ResponseContext responseContext) {
        for (Interceptor interceptor : interceptors) {
            interceptor.afterSendMessage(responseContext);
        }
    }

    public void onException(ExceptionContext exceptionContext) {
        for (Interceptor interceptor : interceptors) {
            interceptor.onException(exceptionContext);
        }
    }
}
