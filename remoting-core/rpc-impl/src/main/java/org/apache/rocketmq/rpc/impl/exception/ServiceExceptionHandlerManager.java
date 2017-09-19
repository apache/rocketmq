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

package org.apache.rocketmq.rpc.impl.exception;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import org.apache.rocketmq.remoting.api.command.RemotingCommand;
import org.apache.rocketmq.remoting.api.serializable.Serializer;
import org.apache.rocketmq.remoting.api.serializable.SerializerFactory;
import org.apache.rocketmq.rpc.impl.command.ResponseCode;
import org.apache.rocketmq.rpc.internal.RpcErrorMapper;

import static org.apache.rocketmq.remoting.internal.ExceptionUtils.getStackTrace;

public class ServiceExceptionHandlerManager {
    private static boolean isIllegalCode(int code) {
        return code > 0;
    }

    private static ServiceExceptionInvokeMessage getInvokeExceptionMessage(RemotingCommand remotingCommand,
        SerializerFactory serializerFactory) {
        Serializer serialization = serializerFactory.get(remotingCommand.serializerType());
        if (remotingCommand.parameterBytes() == null)
            return null;
        return serialization.decode(remotingCommand.parameterBytes(), ServiceExceptionInvokeMessage.class);
    }

    private static Exception getResponseException(String className, RemotingCommand remotingCommand,
        SerializerFactory serializerFactory)
        throws ClassNotFoundException, NoSuchMethodException, IllegalAccessException,
        InstantiationException, InvocationTargetException {
        return parseException(serializerFactory, remotingCommand, className);
    }

    @SuppressWarnings("unchecked")
    private static Exception parseException(SerializerFactory serializerFactory, RemotingCommand remotingCommand,
        String className)
        throws ClassNotFoundException, NoSuchMethodException,
        IllegalAccessException, InvocationTargetException, InstantiationException {
        Class exceptionClass = Class.forName(className);
        assert exceptionClass != null;
        Class[] oneParamTypes = {String.class};
        Class[] twoParamTypes = {String.class, Throwable.class};
        ServiceExceptionInvokeMessage serviceExceptionInvokeMessage = getInvokeExceptionMessage(remotingCommand, serializerFactory);
        if (serviceExceptionInvokeMessage == null)
            return new Exception();

        if (serviceExceptionInvokeMessage.getThrowable() == null) {
            Object[] onsParams = {serviceExceptionInvokeMessage.getErrorMessage()};
            Constructor constructor = exceptionClass.getConstructor(oneParamTypes);
            assert constructor != null;
            return (Exception) constructor.newInstance(onsParams);
        } else {
            Object[] twoParams = {serviceExceptionInvokeMessage.getErrorMessage(), serviceExceptionInvokeMessage.getThrowable()};
            Constructor constructor = exceptionClass.getConstructor(twoParamTypes);
            assert constructor != null;
            return (Exception) constructor.newInstance(twoParams);
        }
    }

    public static void exceptionHandler(String code, RemotingCommand remotingCommand,
        SerializerFactory serializerFactory) throws Exception {
        int intCode;
        try {
            intCode = Integer.parseInt(code);
        } catch (NumberFormatException e) {
            throw ServiceExceptionManager.TranslateException(RpcErrorMapper.RpcErrorLatitude.REMOTE.getCode(), e);
        }

        if (isIllegalCode(intCode)) {
            ServiceExceptionInvokeMessage returnResult = getInvokeExceptionMessage(remotingCommand, serializerFactory);
            if (returnResult == null) {
                ResponseCode.ResponseStatus responseStatus = ServiceExceptionHandlerCode.searchResponseStatus(intCode);
                if (responseStatus != null) {
                    throw new ServiceRuntimeException(RpcErrorMapper.RpcErrorLatitude.REMOTE.getCode(),
                        String.valueOf(responseStatus.getResponseCode()));
                } else {
                    throw new ServiceRuntimeException(RpcErrorMapper.RpcErrorLatitude.REMOTE.getCode(),
                        ResponseCode.UNKNOWN_EXCEPTION);
                }
            }

            try {
                Exception exception = getResponseException(returnResult.getClassFullName(), remotingCommand, serializerFactory);
                if (intCode == Integer.valueOf(ResponseCode.USER_SERVICE_EXCEPTION)) {
                    throw exception;
                } else {
                    throw new ServiceRuntimeException(RpcErrorMapper.RpcErrorLatitude.REMOTE.getCode(),
                        String.valueOf(intCode),
                        getStackTrace(exception));
                }
            } catch (ClassNotFoundException e) {
                throw new ServiceRuntimeException(RpcErrorMapper.RpcErrorLatitude.LOCAL.getCode(),
                    String.valueOf(ResponseCode.USER_EXCEPTION_CLASS_NOT_FOUND),
                    e.getMessage());
            } catch (NoSuchMethodException e) {
                throw new ServiceRuntimeException(RpcErrorMapper.RpcErrorLatitude.REMOTE.getCode(),
                    String.valueOf(ResponseCode.USER_EXCEPTION_METHOD_NOT_FOUND),
                    e.getMessage());
            } catch (IllegalAccessException e) {
                throw new ServiceRuntimeException(RpcErrorMapper.RpcErrorLatitude.REMOTE.getCode(),
                    String.valueOf(ResponseCode.ILLEGAL_ACCESS),
                    e.getMessage());
            } catch (InstantiationException e) {
                throw new ServiceRuntimeException(RpcErrorMapper.RpcErrorLatitude.REMOTE.getCode(),
                    String.valueOf(ResponseCode.INSTANTIATED_FAIL),
                    e.getMessage());
            } catch (InvocationTargetException e) {
                throw new ServiceRuntimeException(RpcErrorMapper.RpcErrorLatitude.REMOTE.getCode(),
                    String.valueOf(ResponseCode.FAIL_INVOKE),
                    e.getMessage());
            }
        } else {
            throw new ServiceRuntimeException(RpcErrorMapper.RpcErrorLatitude.REMOTE.getCode(),
                ResponseCode.UNKNOWN_EXCEPTION);
        }
    }
}
