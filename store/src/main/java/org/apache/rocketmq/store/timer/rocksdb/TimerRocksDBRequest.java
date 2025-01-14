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
package org.apache.rocketmq.store.timer.rocksdb;

import org.apache.rocketmq.common.message.MessageExt;

public class TimerRocksDBRequest {
    MessageExt msgExt;
    boolean needRoll;

    public TimerRocksDBRequest() {
    }

    public TimerRocksDBRequest(MessageExt msgExt, boolean needRoll) {
        this.msgExt = msgExt;
        this.needRoll = needRoll;
    }

    public MessageExt getMsgExt() {
        return msgExt;
    }

    public void setMsgExt(MessageExt msgExt) {
        this.msgExt = msgExt;
    }

    public boolean isNeedRoll() {
        return needRoll;
    }

    public void setNeedRoll(boolean needRoll) {
        this.needRoll = needRoll;
    }

    @Override
    public String toString() {
        return "TimerRocksDBRequest{" +
            "msgExt=" + msgExt +
            ", needRoll=" + needRoll +
            '}';
    }
}
