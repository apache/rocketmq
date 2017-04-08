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
package org.apache.rocketmq.common.sysflag;

/**
 * TODO 疑问
 */
public class TopicSysFlag {

    private final static int FLAG_UNIT = 0x1 << 0;

    private final static int FLAG_UNIT_SUB = 0x1 << 1;

    public static int buildSysFlag(final boolean unit, final boolean hasUnitSub) {
        int sysFlag = 0;

        if (unit) {
            sysFlag |= FLAG_UNIT;
        }

        if (hasUnitSub) {
            sysFlag |= FLAG_UNIT_SUB;
        }

        return sysFlag;
    }

    public static int setUnitFlag(final int sysFlag) {
        return sysFlag | FLAG_UNIT;
    }

    public static int clearUnitFlag(final int sysFlag) {
        return sysFlag & (~FLAG_UNIT);
    }

    public static boolean hasUnitFlag(final int sysFlag) {
        return (sysFlag & FLAG_UNIT) == FLAG_UNIT;
    }

    public static int setUnitSubFlag(final int sysFlag) {
        return sysFlag | FLAG_UNIT_SUB;
    }

    public static int clearUnitSubFlag(final int sysFlag) {
        return sysFlag & (~FLAG_UNIT_SUB);
    }

    public static boolean hasUnitSubFlag(final int sysFlag) {
        return (sysFlag & FLAG_UNIT_SUB) == FLAG_UNIT_SUB;
    }

    public static void main(String[] args) {
    }
}
