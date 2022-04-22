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

package org.apache.rocketmq.apis.consumer;

import org.apache.rocketmq.apis.message.MessageView;

/**
 * <p>MessageListener is used only by PushConsumer to process messages
 * synchronously.
 *
 * <p>PushConsumer will fetch messages from brokers and dispatch them to an
 * embedded thread pool in form of <code>Runnable</code> tasks to achieve
 * desirable processing concurrency.
 *
 * <p>Refer to {@link PushConsumer} for more further specs.
 *
 * <p>
 *     <strong>Thread Safety</strong>
 *     This class may be called concurrently by multiple threads. Implementation should be thread safe.
 * </p>
 */
public interface MessageListener {

  /**
   * Callback interface to handle incoming messages.
   *
   * Application developers are expected to implement this interface to fulfill
   * business requirements through processing <code>message</code> and return
   * <code>ConsumeResult</code> accordingly.
   *
   * PushConsumer will, on behalf of its group, acknowledge the message to
   * broker on success; In case of failure or unexpected exceptions
   * were raised, it will negatively acknowledge <code>message</code>, which
   * would potentially get re-delivered after the configured back off period.
   *
   * @param message The message passed to the listener.
   * @return {@link ConsumeResult#SUCCESS} if <code>message</code> is properly processed; {@link ConsumeResult#FAILURE}
   *         otherwise.
   */
    ConsumeResult onMessage(MessageView message);
}
