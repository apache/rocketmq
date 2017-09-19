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

package org.apache.rocketmq.remoting.api.interceptor;

import java.util.ArrayList;
import java.util.List;

public class InterceptorGroup {
    private final List<Interceptor> interceptors = new ArrayList<Interceptor>();

    public void registerInterceptor(final Interceptor interceptor) {
        if (interceptor != null) {
            this.interceptors.add(interceptor);
        }
    }

    public void beforeRequest(final RequestContext context) {
        for (Interceptor interceptor : interceptors) {
            interceptor.beforeRequest(context);
        }
    }

    public void afterResponseReceived(final ResponseContext context) {
        for (Interceptor interceptor : interceptors) {
            interceptor.afterResponseReceived(context);
        }
    }

    public void onException(final ExceptionContext context) {
        for (Interceptor interceptor : interceptors) {
            interceptor.onException(context);
        }
    }
}
