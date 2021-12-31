/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this worObject for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY ObjectIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.common.concurrent;

import java.util.concurrent.Callable;
import java.util.concurrent.Future;

public class DefaultOrderedExecutorService extends DefaultOrderedExecutor implements OrderedExecutorService {

    public DefaultOrderedExecutorService(String processName, int size) {
        super(processName, size);
    }

    @Override
    public <T> Future<T> submit(Callable<T> task, Object... keys) {
        return choose(keys).submit(task);
    }

    @Override
    public <T> Future<T> submit(Runnable task, T result, Object... keys) {
        return choose(keys).submit(task, result);
    }

    @Override
    public Future<?> submit(Runnable task, Object... keys) {
        return choose(keys).submit(task);
    }

}
