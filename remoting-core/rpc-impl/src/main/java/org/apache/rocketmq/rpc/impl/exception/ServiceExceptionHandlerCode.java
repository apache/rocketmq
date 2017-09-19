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

import java.util.HashMap;
import java.util.Map;
import org.apache.rocketmq.remoting.api.exception.RemoteConnectFailureException;
import org.apache.rocketmq.remoting.api.exception.RemoteTimeoutException;
import org.apache.rocketmq.rpc.impl.command.ResponseCode;

public class ServiceExceptionHandlerCode extends ResponseCode {

    private static Map<Class, Integer> exceptionCodeMap = new HashMap<>();

    static {
        exceptionCodeMap.put(IllegalAccessException.class, 100);
        exceptionCodeMap.put(IllegalArgumentException.class, 101);
        exceptionCodeMap.put(NullPointerException.class, 102);
        exceptionCodeMap.put(InstantiationException.class, 104);
        exceptionCodeMap.put(NumberFormatException.class, 105);
        exceptionCodeMap.put(RemoteTimeoutException.class, 106);
        exceptionCodeMap.put(RemoteConnectFailureException.class, 107);
        exceptionCodeMap.put(ClassNotFoundException.class, 402);
    }

    public static int searchExceptionCode(Class type) {
        Integer code = exceptionCodeMap.get(type);
        if (code == null)
            //Default exception code
            return 100;
        return code;
    }

    public static ResponseStatus searchResponseStatus(int code) {
        for (ResponseStatus responseStatus : ResponseStatus.values()) {
            if (code == responseStatus.getResponseCode())
                return responseStatus;
        }
        return null;
    }
}
