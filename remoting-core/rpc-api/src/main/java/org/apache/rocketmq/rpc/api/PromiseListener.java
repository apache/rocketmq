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

package org.apache.rocketmq.rpc.api;

/**
 * A listener that is called back when a Promise is done.
 * {@code PromiseListener} instances are attached to {@link Promise} by passing
 * them in to {@link Promise#addListener(PromiseListener)}.
 *
 * @since 1.0.0
 */
public interface PromiseListener<V> {
    /**
     * Invoked when the operation associated with the {@code Promise} has been completed successfully.
     *
     * @param promise the source {@code Promise} which called this callback
     */
    void operationCompleted(Promise<V> promise);

    /**
     * Invoked when the operation associated with the {@code Promise} has been completed unsuccessfully.
     *
     * @param promise the source {@code Promise} which called this callback
     */
    void operationFailed(Promise<V> promise);
}
