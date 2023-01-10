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
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.rocketmq.test.listener.rmq.concurrent;

import java.util.List;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.common.message.MessageExt;

public class RMQBlockListener extends RMQNormalListener {
    private volatile boolean block = true;
    private volatile boolean inBlock = true;

    public RMQBlockListener() {
        super();
    }

    public RMQBlockListener(boolean block) {
        super();
        this.block = block;
    }

    public boolean isBlocked() {
        return inBlock;
    }

    public void setBlock(boolean block) {
        this.block = block;
    }

    @Override
    public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
        ConsumeConcurrentlyStatus status = super.consumeMessage(msgs, context);

        try {
            while (block) {
                inBlock = true;
                Thread.sleep(100);
            }
        } catch (InterruptedException ignore) {
        }

        return status;
    }
}
