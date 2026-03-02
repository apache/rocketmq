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
package org.apache.rocketmq.common.chain;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class HandlerChain<T, R> {

    private List<Handler<T, R>> handlers;
    private Iterator<Handler<T, R>> iterator;

    public static <T, R> HandlerChain<T, R> create() {
        return new HandlerChain<>();
    }

    public HandlerChain<T, R> addNext(Handler<T, R> handler) {
        if (this.handlers == null) {
            this.handlers = new ArrayList<>();
        }
        this.handlers.add(handler);
        return this;
    }

    public R handle(T t) {
        if (iterator == null) {
            iterator = handlers.iterator();
        }
        if (iterator.hasNext()) {
            Handler<T, R> handler = iterator.next();
            return handler.handle(t, this);
        }
        return null;
    }
}
