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

package org.apache.rocketmq.rpc.impl.processor;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import org.apache.rocketmq.remoting.api.RequestProcessor;
import org.apache.rocketmq.remoting.api.channel.RemotingChannel;
import org.apache.rocketmq.remoting.api.command.RemotingCommand;
import org.apache.rocketmq.remoting.api.serializable.Serializer;
import org.apache.rocketmq.rpc.annotation.RemoteService;
import org.apache.rocketmq.rpc.impl.command.ResponseCode;
import org.apache.rocketmq.rpc.impl.context.RpcProviderContext;
import org.apache.rocketmq.rpc.impl.exception.ServiceExceptionInvokeMessage;
import org.apache.rocketmq.rpc.impl.metrics.ServiceStats;
import org.apache.rocketmq.rpc.impl.service.RpcEntry;
import org.apache.rocketmq.rpc.impl.service.RpcInstanceAbstract;
import org.apache.rocketmq.rpc.impl.service.RpcServiceCallBody;
import org.apache.rocketmq.rpc.internal.ServiceUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RpcRequestProcessor implements RequestProcessor {
    private static final Logger log = LoggerFactory.getLogger(RpcRequestProcessor.class);
    private Map<String, RpcEntry> serviceTable = new ConcurrentHashMap<>(64);
    private Map<String, ExecutorService> executorTable = new ConcurrentHashMap<>(64);
    private ThreadLocal<RpcProviderContext> threadLocalProviderContext;
    private RpcInstanceAbstract rpcInstanceAbstract;
    private ServiceStats serviceStats;

    public RpcRequestProcessor(ThreadLocal<RpcProviderContext> threadLocalProviderContext,
        final RpcInstanceAbstract rpcInstanceAbstract, ServiceStats stats) {
        this.threadLocalProviderContext = threadLocalProviderContext;
        this.rpcInstanceAbstract = rpcInstanceAbstract;
        this.serviceStats = stats;
    }

    public Set<String> putNewService(final Object obj) {
        return putNewService(obj, null);
    }

    public Set<String> putNewService(final Object obj, final ExecutorService executorService) {
        Class<?>[] interfaces = obj.getClass().getInterfaces();
        for (Class<?> itf : interfaces) {
            RemoteService serviceExport = itf.getAnnotation(RemoteService.class);
            if (null == serviceExport) {
                log.warn("Service:{} is not remark annotation", itf.getName());
                continue;
            }

            Method[] methods = itf.getMethods();
            for (Method method : methods) {
                if (!ServiceUtil.testServiceExportMethod(method)) {
                    log.error("The method: [{}] not matched RPC standard", method.toGenericString());
                    continue;
                }

                String requestCode = ServiceUtil.toRequestCode(serviceExport, method);
                RpcEntry se = new RpcEntry();
                se.setServiceExport(serviceExport);
                se.setObject(obj);
                se.setMethod(method);
                this.serviceTable.put(requestCode, se);
                if (executorService != null) {
                    this.executorTable.put(requestCode, executorService);
                }
            }
        }
        return this.serviceTable.keySet();
    }

    @Override
    public RemotingCommand processRequest(RemotingChannel channel, RemotingCommand request) {
        RpcProviderContext rpcProviderContext = new RpcProviderContext();
        rpcProviderContext.setRemotingChannel(channel);
        rpcProviderContext.setRemotingRequest(request);
        rpcProviderContext.setRemotingResponse(rpcInstanceAbstract.remotingService().commandFactory().createResponse(request));
        rpcProviderContext.setReturnResponse(true);
        threadLocalProviderContext.set(rpcProviderContext);
        this.processRequest0(rpcProviderContext, request);
        threadLocalProviderContext.remove();

        if (rpcProviderContext.isReturnResponse()) {
            return rpcProviderContext.getRemotingResponse();
        }
        return null;
    }

    private Object[] buildParameter(final RpcProviderContext context, final RemotingCommand request,
        RpcServiceCallBody serviceCallBody, Type[] parameterTypes) {
        Object[] parameters = new Object[parameterTypes.length];
        try {
            int index = 0;
            Serializer serialization = this.rpcInstanceAbstract.remotingService()
                .serializerFactory().get(request.serializerType());
            for (Type parameterType : parameterTypes) {
                parameters[index] = serialization.decode(serviceCallBody.getParameter(index), parameterType);
                index++;
            }
        } catch (Exception e) {
            ServiceExceptionInvokeMessage serviceExceptionInvokeMessage = new ServiceExceptionInvokeMessage();
            serviceExceptionInvokeMessage.setClassFullName(e.getClass().getName());
            serviceExceptionInvokeMessage.setErrorMessage(serializeException(request.serializerType(), e));
            context.getRemotingResponse().opCode(ResponseCode.PARAMETER_ERROR);
            context.getRemotingResponse().parameter(serviceExceptionInvokeMessage);
        }
        return parameters;
    }

    private String serializeException(byte serializeType, Exception exception) {
        Serializer serialization = rpcInstanceAbstract.remotingService().serializerFactory().get(serializeType);
        return serialization.encode(exception).toString();
    }

    private void dealWithException(final RpcProviderContext context, RemotingCommand request, Exception exception) {
        if (exception instanceof InvocationTargetException) {
            context.getRemotingResponse().opCode(ResponseCode.USER_SERVICE_EXCEPTION);
        } else if (exception instanceof IllegalArgumentException) {
            context.getRemotingResponse().opCode(ResponseCode.PARAMETER_ERROR);
        } else if (exception instanceof IllegalAccessException) {
            context.getRemotingResponse().opCode(ResponseCode.ILLEGAL_ACCESS);
        } else if (exception instanceof NullPointerException) {
            context.getRemotingResponse().opCode(ResponseCode.NULL_POINTER);
        } else {
            context.getRemotingResponse().opCode(ResponseCode.USER_SERVICE_EXCEPTION);
        }
        String remarkMsg, exceptionMsg, exceptionClassname;
        ServiceExceptionInvokeMessage serviceExceptionInvokeMessage = new ServiceExceptionInvokeMessage();
        if (exception instanceof InvocationTargetException) {
            exceptionMsg = ((InvocationTargetException) exception).getTargetException().getMessage();
            remarkMsg = exceptionMsg == null ? "" : exceptionMsg;
            exceptionClassname = ((InvocationTargetException) exception).getTargetException().getClass().getName();
        } else {
            exceptionMsg = exception.getMessage();
            remarkMsg = exceptionMsg == null ? "" : exceptionMsg;
            exceptionClassname = exception.getClass().getName();
        }
        serviceExceptionInvokeMessage.setClassFullName(exceptionClassname);
        serviceExceptionInvokeMessage.setErrorMessage(remarkMsg);
        if (exception.getCause() != null)
            serviceExceptionInvokeMessage.setThrowable(exception.getCause());
        Object[] args = new Object[] {serviceExceptionInvokeMessage};
        Serializer serializer = ServiceUtil.selectSerializer(args, request.serializerType());
        if (serializer != null)
            context.getRemotingResponse().serializerType(serializer.type());
        context.getRemotingResponse().parameter(serviceExceptionInvokeMessage);
    }

    private void processRequest0(final RpcProviderContext context, final RemotingCommand request) {
        final RpcServiceCallBody serviceCallBody =
            request.parameter(this.rpcInstanceAbstract.remotingService().serializerFactory(), RpcServiceCallBody.class);
        final RpcEntry entry = this.serviceTable.get(serviceCallBody.getServiceId());
        if (entry != null) {
            Runnable runnable = new Runnable() {
                @Override
                public void run() {
                    Type[] parameterTypes = entry.getMethod().getGenericParameterTypes();
                    Object result = null;
                    Exception exception = null;
                    long startTime = System.currentTimeMillis();
                    try {
                        if (parameterTypes.length == 0) {
                            result = entry.getMethod().invoke(entry.getObject());
                        } else {
                            Object[] parameters = buildParameter(context, request, serviceCallBody, parameterTypes);
                            result = entry.getMethod().invoke(entry.getObject(), parameters);
                        }
                        serviceStats.addProviderOKQPSValue(serviceCallBody.getServiceId(), 1, 1);
                        serviceStats.addProviderRTValue(serviceCallBody.getServiceId(), (int) (System.currentTimeMillis() - startTime), 1);
                    } catch (Exception e) {
                        exception = e;
                    }

                    if (exception != null)
                        dealWithException(context, request, exception);
                    else if (!entry.getMethod().getReturnType().equals(Void.class)) {
                        Object[] args = new Object[] {result};
                        Serializer serializer = ServiceUtil.selectSerializer(args, request.serializerType());
                        if (serializer != null)
                            context.getRemotingResponse().serializerType(serializer.type());
                        context.getRemotingResponse().parameter(result);
                    }
                }
            };
            ExecutorService executorService = this.executorTable.get(serviceCallBody.getServiceId());
            if (executorService != null) {
                executorService.submit(runnable);
            } else {
                runnable.run();
            }
        }
    }
}
