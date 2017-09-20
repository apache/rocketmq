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

import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.nio.ByteBuffer;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.remoting.api.AsyncHandler;
import org.apache.rocketmq.remoting.api.RemotingService;
import org.apache.rocketmq.remoting.api.command.RemotingCommand;
import org.apache.rocketmq.remoting.api.serializable.Serializer;
import org.apache.rocketmq.remoting.api.serializable.SerializerFactory;
import org.apache.rocketmq.remoting.external.ThreadUtils;
import org.apache.rocketmq.remoting.impl.protocol.serializer.SerializerFactoryImpl;
import org.apache.rocketmq.rpc.annotation.MethodType;
import org.apache.rocketmq.rpc.annotation.RemoteMethod;
import org.apache.rocketmq.rpc.annotation.RemoteService;
import org.apache.rocketmq.rpc.annotation.VisibleForInternal;
import org.apache.rocketmq.rpc.api.Promise;
import org.apache.rocketmq.rpc.api.PromiseListener;
import org.apache.rocketmq.rpc.impl.command.ResponseCode;
import org.apache.rocketmq.rpc.impl.command.RpcRequestCode;
import org.apache.rocketmq.rpc.impl.config.RpcCommonConfig;
import org.apache.rocketmq.rpc.impl.context.RpcCallerContext;
import org.apache.rocketmq.rpc.impl.exception.ServiceExceptionHandlerManager;
import org.apache.rocketmq.rpc.impl.exception.ServiceExceptionManager;
import org.apache.rocketmq.rpc.impl.metrics.ServiceStats;
import org.apache.rocketmq.rpc.impl.promise.DefaultPromise;
import org.apache.rocketmq.rpc.internal.ExceptionMessageUtil;
import org.apache.rocketmq.rpc.internal.RpcErrorMapper;
import org.apache.rocketmq.rpc.internal.ServiceUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.rocketmq.remoting.internal.ExceptionUtils.getStackTrace;

public abstract class RpcProxyCommon {
    protected final static SerializerFactory SERIALIZER_FACTORY = new SerializerFactoryImpl();
    protected static final Logger log = LoggerFactory.getLogger(RpcProxyCommon.class);
    protected final ThreadLocal<RpcCallerContext> threadLocalCallerContext = new ThreadLocal<>();
    protected RpcCommonConfig rpcCommonConfig;
    protected ExecutorService callServiceThreadPool;
    protected ExecutorService promiseExecutorService;
    protected ServiceStats serviceStats;

    public RpcProxyCommon(RpcCommonConfig rpcCommonConfig) {
        this.rpcCommonConfig = rpcCommonConfig;
        this.serviceStats = new ServiceStats();
        this.promiseExecutorService = ThreadUtils.newFixedThreadPool(
            rpcCommonConfig.getClientAsyncCallbackExecutorThreads(),
            rpcCommonConfig.getServiceThreadBlockQueueSize(),
            "Remoting-PromiseExecutorService", true);
        this.callServiceThreadPool = ThreadUtils.newFixedThreadPool(
            rpcCommonConfig.getClientAsyncCallbackExecutorThreads(),
            rpcCommonConfig.getServiceThreadBlockQueueSize(),
            "Remoting-CallServiceThread", true);
    }

    private RemotingCommand createRemoteRequest(RemoteService serviceExport, Method method, Object[] args,
        RemoteMethod asynchronous) {
        RemotingCommand request;
        VisibleForInternal visible = method.getAnnotation(VisibleForInternal.class);
        if (visible != null) {
            request = remotingService().commandFactory().createRequest();
            request.opCode(asynchronous.name());
            if (args != null && args.length != 0) {
                request.parameter(args[0]);
            }
        } else {
            String requestCode = ServiceUtil.toRequestCode(serviceExport, method);
            request = createRemoteRequest(requestCode, asynchronous.version(), args);
        }
        return request;
    }

    public RemotingCommand createRemoteRequest(String requestCode, String version, Object[] args) {
        RemotingCommand request = remotingService().commandFactory().createRequest();
        request.opCode(RpcRequestCode.CALL_SERVICE);
        RpcServiceCallBody serviceCallBody = new RpcServiceCallBody();
        serviceCallBody.setServiceId(requestCode);

        if (args != null) {
            Serializer serializer = ServiceUtil.selectSerializer(args, SERIALIZER_FACTORY.type(rpcCommonConfig.getSerializerName()));
            assert serializer != null;
            request.serializerType(serializer.type());
            for (Object arg : args) {
                ByteBuffer byteBuffer = serializer.encode(arg);
                byte[] array = new byte[byteBuffer.remaining()];
                byteBuffer.get(array);
                serviceCallBody.addParameter(array);
            }
        }
        serviceCallBody.setServiceVersion(version);
        request.parameter(serviceCallBody);
        return request;
    }

    @SuppressWarnings("unchecked")
    Object invoke0(Object proxy, RpcJdkProxy rpcJdkProxy, Class<?> service, final Method method,
        Object[] args) throws Throwable {
        RemoteMethod asynchronous = getRemoteMethod(method);
        final String requestCode = createRequestCode(method, service);
        RemoteService serviceExport = getRemoteService(service);
        RemotingCommand request = createRemoteRequest(serviceExport, method, args, asynchronous);
        return invokeRemoteMethod(rpcJdkProxy, requestCode, request, method.getGenericReturnType(), asynchronous.type());
    }

    public Object invokeRemoteMethod(final RpcJdkProxy rpcJdkProxy, final String requestCode,
        RemotingCommand request, final Type returnType, MethodType type) {
        try {
            final long startTime = System.currentTimeMillis();
            InvokeRunnable invokeRunnable = new InvokeRunnable(rpcJdkProxy, requestCode, request, type);
            final Future future = callServiceThreadPool.submit(invokeRunnable);
            /*
            final Promise result = callService(method, args);
            */
            final DefaultPromise resPromise = new DefaultPromise();
            Object result = future.get(rpcCommonConfig.getClientInvokeServiceTimeout(), TimeUnit.MILLISECONDS);
            if (result == null || !(result instanceof Promise)) {
                throw ServiceExceptionManager.TranslateException(RpcErrorMapper.RpcErrorLatitude.LOCAL.getCode(),
                    new Exception("illegal response type is return"));
            } else if (result instanceof Exception) {
                throw ServiceExceptionManager.TranslateException(RpcErrorMapper.RpcErrorLatitude.LOCAL.getCode(),
                    new Exception((Throwable) result));
            }

            final DefaultPromise promise = (DefaultPromise) result;
            switch (type) {
                case SYNC:
                    final Object response = promise.get(rpcCommonConfig.getClientInvokeServiceTimeout());
                    if (response == null) {
                        throw new Exception("get result is null");
                    }
                    if (response instanceof Exception)
                        throw (Exception) response;
                    return processResponse(returnType, (RemotingCommand) response,
                        requestCode, startTime);
                case ASYNC:
                    promiseExecutorService.submit(new Runnable() {
                        @Override
                        public void run() {
                            promise.addListener(new PromiseListener() {
                                @Override
                                public void operationCompleted(Promise promise) {
                                    Object response = promise.get();
                                    if (response == null) {
                                        resPromise.setFailure(new Exception("get result is null"));
                                        return;
                                    }
                                    if (response instanceof Exception) {
                                        resPromise.setFailure((Exception) response);
                                        return;
                                    }
                                    Type restyle = ((ParameterizedType) returnType).getActualTypeArguments()[0];
                                    try {
                                        resPromise.set(processResponse(restyle, (RemotingCommand) response, requestCode, startTime));
                                    } catch (Exception e) {
                                        resPromise.setFailure(ServiceExceptionManager.TranslateException(RpcErrorMapper.RpcErrorLatitude.LOCAL.getCode(), e));
                                    }
                                }

                                @Override
                                public void operationFailed(Promise promise) {
                                    resPromise.setFailure(ServiceExceptionManager.TranslateException(RpcErrorMapper.RpcErrorLatitude.LOCAL.getCode(), promise.getThrowable()));
                                }
                            });
                        }
                    });
                    return resPromise;
            }
            throw ServiceExceptionManager.TranslateException(RpcErrorMapper.RpcErrorLatitude.LOCAL.getCode(), new Exception("illegal response type is return"));
        } catch (ExecutionException e) {
            throw ServiceExceptionManager.TranslateException(RpcErrorMapper.RpcErrorLatitude.LOCAL.getCode(), e.getCause());
        } catch (Exception e) {
            throw ServiceExceptionManager.TranslateException(RpcErrorMapper.RpcErrorLatitude.LOCAL.getCode(), e);
        }
    }

    private Object processResponse(Type returnType, RemotingCommand remotingCommand,
        String requestCode, long startTime) throws Exception {
        RpcCallerContext rpcCallerContext = this.threadLocalCallerContext.get();
        if (rpcCallerContext != null) {
            rpcCallerContext.setRemotingResponse(remotingCommand);
        }

        String code = remotingCommand.opCode();
        if (code.equals(ResponseCode.SUCCESS)) {
            serviceStats.addCallerOKQPSValue(requestCode, 1, 1);
            serviceStats.addCallerRTValue(requestCode, (int) (System.currentTimeMillis() - startTime), 1);
            try {
                if (!returnType.equals(Void.class)) {
                    Serializer serialization = remotingService().serializerFactory().get(remotingCommand.serializerType());
                    if (remotingCommand.parameterBytes() == null)
                        return null;
                    return serialization.decode(remotingCommand.parameterBytes(), returnType);
                } else {
                    return Void.class.newInstance();
                }
            } catch (Exception e) {
                throw ServiceExceptionManager.TranslateException(RpcErrorMapper.RpcErrorLatitude.LOCAL.getCode(), e);
            }
        } else {
            log.error("process response is failed, errorCode:{}, remark:{}", code, remotingCommand.remark());
            serviceStats.addCallerFailedQPSValue(requestCode, 1, 1);
            serviceStats.addCallerRTValue(requestCode, (int) (System.currentTimeMillis() - startTime), 1);
            ServiceExceptionHandlerManager.exceptionHandler(code, remotingCommand, remotingService().serializerFactory());
            return null;
        }
    }

    private String createRequestCode(Method method, Class<?> service) {
        return ServiceUtil.toRequestCode(getRemoteService(service), method);
    }

    private RemoteService getRemoteService(Class<?> service) {
        RemoteService serviceExport = service.getAnnotation(RemoteService.class);
        if (serviceExport == null) {
            throw new IllegalArgumentException("ServiceExport:" + service.getName() + " do not remark annotation");
        }
        return serviceExport;
    }

    private RemoteMethod getRemoteMethod(Method method) {
        RemoteMethod asynchronous = method.getAnnotation(RemoteMethod.class);
        if (asynchronous == null) {
            throw new IllegalArgumentException("ServiceMethod:" + method.getName() + " do not remark annotation");
        }
        return asynchronous;
    }

    private Promise invokeAsyncWrapper(final RpcJdkProxy rpcJdkProxy, RemotingCommand request,
        final String requestCode) {
        final RpcCallerContext rpcCallerContext = this.threadLocalCallerContext.get();
        final DefaultPromise promise = new DefaultPromise();
        final long startTime = System.currentTimeMillis();
        rpcJdkProxy.invokeAsync(request, new AsyncHandler() {
            @Override
            public void onFailure(RemotingCommand request) {
                serviceStats.addCallerFailedQPSValue(requestCode, 1, 1);
                serviceStats.addCallerRTValue(requestCode, (int) (System.currentTimeMillis() - startTime), 1);
                promise.setFailure(new Exception(ExceptionMessageUtil.buildExceptionMessage("request:" + request.toString() + " onFailure")));
            }

            @Override
            public void onTimeout(long costTimeMillis, long timeoutMillis) {
                serviceStats.addCallerFailedQPSValue(requestCode, 1, 1);
                serviceStats.addCallerRTValue(requestCode, (int) (System.currentTimeMillis() - startTime), 1);
                promise.setFailure(new Exception(ExceptionMessageUtil.buildExceptionMessage("service timeout")));
            }

            @Override
            public void onSuccess(RemotingCommand response) {
                if (null != rpcCallerContext) {
                    rpcCallerContext.setRemotingResponse(response);
                }
                try {
                    promise.set(response);
                } catch (Throwable e) {
                    promise.setFailure(new Exception(ExceptionMessageUtil.buildExceptionMessage(getStackTrace(e))));
                }
            }

        });
        return promise;
    }

    public abstract RemotingService remotingService();

    public RpcCommonConfig getRpcCommonConfig() {
        return rpcCommonConfig;
    }

    public void setRpcCommonConfig(final RpcCommonConfig rpcCommonConfig) {
        this.rpcCommonConfig = rpcCommonConfig;
    }

    private class InvokeRunnable implements Callable {
        private RpcJdkProxy rpcJdkProxy;
        private String requestCode;
        private RemotingCommand request;
        private MethodType type;

        InvokeRunnable(RpcJdkProxy rpcJdkProxy, String requestCode, RemotingCommand request,
            MethodType type) {
            this.rpcJdkProxy = rpcJdkProxy;
            this.requestCode = requestCode;
            this.request = request;
            this.type = type;
        }

        @SuppressWarnings("unchecked")
        @Override
        public Object call() {
            Promise promise = new DefaultPromise();

            try {
                if (type == MethodType.ONEWAY) {
                    rpcJdkProxy.invokeOneWay(request);
                } else {
                    return RpcProxyCommon.this.invokeAsyncWrapper(rpcJdkProxy, request, requestCode);
                }
            } catch (Exception e) {
                promise.set(ServiceExceptionManager.TranslateException(RpcErrorMapper.RpcErrorLatitude.LOCAL.getCode(), e));
            }
            return promise;

        }
    }
}
