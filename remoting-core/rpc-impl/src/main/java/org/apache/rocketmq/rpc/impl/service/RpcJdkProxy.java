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

package org.apache.rocketmq.rpc.impl.service;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import org.apache.rocketmq.remoting.api.AsyncHandler;
import org.apache.rocketmq.remoting.api.command.RemotingCommand;
import org.apache.rocketmq.rpc.impl.exception.ServiceExceptionManager;
import org.apache.rocketmq.rpc.internal.RpcErrorMapper;

public abstract class RpcJdkProxy implements InvocationHandler {
    private final Class<?> service;
    private final RpcProxyCommon rpcProxyCommon;

    public RpcJdkProxy(final Class<?> service, final RpcProxyCommon rpcProxyCommon) {
        this.service = service;
        this.rpcProxyCommon = rpcProxyCommon;
    }

    @Override
    public Object invoke(final Object proxy, final Method method, final Object[] args) throws Throwable {
        return rpcProxyCommon.invoke0(proxy, this, service, method, args);
    }

    @SuppressWarnings("unchecked")
    public <T> T newInstance(Class<T> service) {
        try {
            return (T) Proxy.newProxyInstance(service.getClassLoader(), new Class[] {service}, this);
        } catch (Exception e) {
            throw ServiceExceptionManager.TranslateException(RpcErrorMapper.RpcErrorLatitude.LOCAL.getCode(), e);
        }
    }

    public abstract void invokeOneWay(final RemotingCommand request);

    public abstract void invokeAsync(final RemotingCommand request, final AsyncHandler handler);
}
