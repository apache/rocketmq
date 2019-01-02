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
package org.apache.rocketmq.snode.interceptor;

import java.util.List;
import org.apache.rocketmq.remoting.util.ServiceProvider;

public class InterceptorFactory {
    private static InterceptorFactory ourInstance = new InterceptorFactory();

    public static InterceptorFactory getInstance() {
        return ourInstance;
    }

    private final String SEND_MESSAGE_INTERCEPTOR = "META-INF/service/org.apache.rocketmq.snode.interceptor.SendMessageInterceptor";

    private final String CONSUME_MESSAGE_INTERCEPTOR = "META-INF/service/org.apache.rocketmq.snode.interceptor.ConsumeMessageInterceptor";

    private InterceptorFactory() {
    }

    public List loadConsumeMessageInterceptors() {
        List<Interceptor> consumeMessageInterceptors = ServiceProvider.loadServiceList(CONSUME_MESSAGE_INTERCEPTOR, Interceptor.class);
        return consumeMessageInterceptors;
    }

    public List loadSendMessageInterceptors() {
        List<Interceptor> sendMessageInterceptors = ServiceProvider.loadServiceList(SEND_MESSAGE_INTERCEPTOR, Interceptor.class);
        return sendMessageInterceptors;
    }

}
