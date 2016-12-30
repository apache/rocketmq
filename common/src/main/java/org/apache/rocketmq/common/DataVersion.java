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
package org.apache.rocketmq.common;

import java.util.concurrent.atomic.AtomicLong;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;

public class DataVersion extends RemotingSerializable {
    private long timestatmp = System.currentTimeMillis();
    private AtomicLong counter = new AtomicLong(0);

    public void assignNewOne(final DataVersion dataVersion) {
        this.timestatmp = dataVersion.timestatmp;
        this.counter.set(dataVersion.counter.get());
    }

    public void nextVersion() {
        this.timestatmp = System.currentTimeMillis();
        this.counter.incrementAndGet();
    }

    public long getTimestatmp() {
        return timestatmp;
    }

    public void setTimestatmp(long timestatmp) {
        this.timestatmp = timestatmp;
    }

    public AtomicLong getCounter() {
        return counter;
    }

    public void setCounter(AtomicLong counter) {
        this.counter = counter;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        final DataVersion that = (DataVersion) o;

        if (timestatmp != that.timestatmp)
            return false;
        return counter != null ? counter.equals(that.counter) : that.counter == null;

    }

    @Override
    public int hashCode() {
        int result = (int) (timestatmp ^ (timestatmp >>> 32));
        result = 31 * result + (counter != null ? counter.hashCode() : 0);
        return result;
    }
}
