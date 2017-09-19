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

package org.apache.rocketmq.remoting.internal;

import java.nio.ByteBuffer;
import java.util.Calendar;

public class UIDGenerator {
    private static ThreadLocal<UIDGenerator> generatorLocal = new ThreadLocal<UIDGenerator>() {
        @Override
        protected UIDGenerator initialValue() {
            return new UIDGenerator();
        }
    };
    private short counter;
    private int basePos = 0;
    private long startTime;
    private long nextStartTime;
    private StringBuilder sb = null;
    private ByteBuffer buffer = ByteBuffer.allocate(6);

    private UIDGenerator() {
        int len = 4 + 2 + 4 + 4 + 2;

        sb = new StringBuilder(len * 2);
        ByteBuffer tempBuffer = ByteBuffer.allocate(len - buffer.limit());
        tempBuffer.position(2);
        tempBuffer.putInt(JvmUtils.getProcessId());
        tempBuffer.position(0);
        try {
            tempBuffer.put((byte) 1);
        } catch (Exception e) {
            tempBuffer.put(createFakeIP());
        }
        tempBuffer.position(6);
        tempBuffer.putInt(UIDGenerator.class.getClassLoader().hashCode());
        sb.append(ByteUtils.toHexString(tempBuffer.array()));
        basePos = sb.length();
        setStartTime(System.currentTimeMillis());
        counter = 0;
    }

    public static UIDGenerator instance() {
        return generatorLocal.get();
    }

    public String createUID() {
        long current = System.currentTimeMillis();
        if (current >= nextStartTime) {
            setStartTime(current);
        }
        buffer.position(0);
        sb.setLength(basePos);
        buffer.putInt((int) (System.currentTimeMillis() - startTime));
        buffer.putShort(counter++);
        sb.append(ByteUtils.toHexString(buffer.array()));
        return sb.toString();
    }

    private void setStartTime(long millis) {
        Calendar cal = Calendar.getInstance();
        cal.setTimeInMillis(millis);
        cal.set(Calendar.DAY_OF_MONTH, 1);
        cal.set(Calendar.HOUR, 0);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);
        startTime = cal.getTimeInMillis();
        cal.add(Calendar.MONTH, 1);
        nextStartTime = cal.getTimeInMillis();
    }

    public byte[] createFakeIP() {
        ByteBuffer bb = ByteBuffer.allocate(8);
        bb.putLong(System.currentTimeMillis());
        bb.position(4);
        byte[] fakeIP = new byte[4];
        bb.get(fakeIP);
        return fakeIP;
    }
}
    
