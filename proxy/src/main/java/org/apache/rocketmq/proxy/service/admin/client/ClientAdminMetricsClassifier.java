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
package org.apache.rocketmq.proxy.service.admin.client;

import java.util.NoSuchElementException;
import org.apache.rocketmq.auth.authentication.exception.AuthenticationException;
import org.apache.rocketmq.auth.authorization.exception.AuthorizationException;
import org.apache.rocketmq.remoting.exception.RemotingTimeoutException;

final class ClientAdminMetricsClassifier {

    private ClientAdminMetricsClassifier() {
    }

    static ClientAdminMetricsResult classify(RuntimeException exception) {
        if (hasCause(exception, IllegalArgumentException.class)) {
            return ClientAdminMetricsResult.BAD_REQUEST;
        }
        if (hasCause(exception, NoSuchElementException.class)) {
            return ClientAdminMetricsResult.NOT_FOUND;
        }
        if (hasCause(exception, AuthenticationException.class)
            || hasCause(exception, AuthorizationException.class)) {
            return ClientAdminMetricsResult.UNAUTHORIZED;
        }
        if (hasCause(exception, RemotingTimeoutException.class)) {
            return ClientAdminMetricsResult.TIMEOUT;
        }
        return ClientAdminMetricsResult.INTERNAL_ERROR;
    }

    private static boolean hasCause(Throwable throwable, Class<? extends Throwable> causeType) {
        Throwable cursor = throwable;
        while (cursor != null) {
            if (causeType.isInstance(cursor)) {
                return true;
            }
            if (cursor.getCause() == cursor) {
                return false;
            }
            cursor = cursor.getCause();
        }
        return false;
    }
}
