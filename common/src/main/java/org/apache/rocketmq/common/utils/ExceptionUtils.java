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
package org.apache.rocketmq.common.utils;

import java.util.Objects;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;

public class ExceptionUtils {
    private ExceptionUtils() {
        // Prevent class from being instantiated from outside
    }

    public static Throwable getRealException(Throwable throwable) {
        if ((throwable instanceof CompletionException || throwable instanceof ExecutionException)
                && Objects.nonNull(throwable.getCause())) {
            throwable = throwable.getCause();
        }

        return throwable;
    }

    public static String getErrorDetailMessage(Throwable t) {
        if (t == null) {
            return null;
        }
        StringBuilder sb = new StringBuilder();
        sb.append(t.getMessage()).append(". ").append(t.getClass().getSimpleName());

        if (t.getStackTrace().length > 0) {
            sb.append(". ").append(t.getStackTrace()[0]);
        }
        return sb.toString();
    }
}
