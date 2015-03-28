/**
 * Copyright (C) 2010-2013 Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.rocketmq.common;

/**
 * 定义各个版本信息
 * 
 * @author shijia.wxr<vintage.wang@gmail.com>
 */
public class MQVersion {
    // TODO 每次发布版本都要修改此处版本号
    public static final int CurrentVersion = Version.V3_2_6.ordinal();


    public static String getVersionDesc(int value) {
        try {
            Version v = Version.values()[value];
            return v.name();
        }
        catch (Exception e) {
        }

        return "HigherVersion";
    }


    public static Version value2Version(int value) {
        return Version.values()[value];
    }

    public static enum Version {
        V3_0_0_SNAPSHOT,
        V3_0_0_ALPHA1,
        V3_0_0_BETA1,
        V3_0_0_BETA2,
        V3_0_0_BETA3,
        V3_0_0_BETA4,
        V3_0_0_BETA5,
        V3_0_0_BETA6_SNAPSHOT,
        V3_0_0_BETA6,
        V3_0_0_BETA7_SNAPSHOT,
        V3_0_0_BETA7,
        V3_0_0_BETA8_SNAPSHOT,
        V3_0_0_BETA8,
        V3_0_0_BETA9_SNAPSHOT,
        V3_0_0_BETA9,
        V3_0_0_FINAL,
        V3_0_1_SNAPSHOT,
        V3_0_1,
        V3_0_2_SNAPSHOT,
        V3_0_2,
        V3_0_3_SNAPSHOT,
        V3_0_3,
        V3_0_4_SNAPSHOT,
        V3_0_4,
        V3_0_5_SNAPSHOT,
        V3_0_5,
        V3_0_6_SNAPSHOT,
        V3_0_6,
        V3_0_7_SNAPSHOT,
        V3_0_7,
        V3_0_8_SNAPSHOT,
        V3_0_8,
        V3_0_9_SNAPSHOT,
        V3_0_9,

        V3_0_10_SNAPSHOT,
        V3_0_10,

        V3_0_11_SNAPSHOT,
        V3_0_11,

        V3_0_12_SNAPSHOT,
        V3_0_12,

        V3_0_13_SNAPSHOT,
        V3_0_13,

        V3_0_14_SNAPSHOT,
        V3_0_14,

        V3_0_15_SNAPSHOT,
        V3_0_15,

        V3_1_0_SNAPSHOT,
        V3_1_0,

        V3_1_1_SNAPSHOT,
        V3_1_1,

        V3_1_2_SNAPSHOT,
        V3_1_2,

        V3_1_3_SNAPSHOT,
        V3_1_3,

        V3_1_4_SNAPSHOT,
        V3_1_4,

        V3_1_5_SNAPSHOT,
        V3_1_5,

        V3_1_6_SNAPSHOT,
        V3_1_6,

        V3_1_7_SNAPSHOT,
        V3_1_7,

        V3_1_8_SNAPSHOT,
        V3_1_8,

        V3_1_9_SNAPSHOT,
        V3_1_9,

        V3_2_0_SNAPSHOT,
        V3_2_0,

        V3_2_1_SNAPSHOT,
        V3_2_1,

        V3_2_2_SNAPSHOT,
        V3_2_2,

        V3_2_3_SNAPSHOT,
        V3_2_3,

        V3_2_4_SNAPSHOT,
        V3_2_4,

        V3_2_5_SNAPSHOT,
        V3_2_5,

        V3_2_6_SNAPSHOT,
        V3_2_6,

        V3_2_7_SNAPSHOT,
        V3_2_7,

        V3_2_8_SNAPSHOT,
        V3_2_8,

        V3_2_9_SNAPSHOT,
        V3_2_9,

        V3_3_1_SNAPSHOT,
        V3_3_1,

        V3_3_2_SNAPSHOT,
        V3_3_2,

        V3_3_3_SNAPSHOT,
        V3_3_3,

        V3_3_4_SNAPSHOT,
        V3_3_4,

        V3_3_5_SNAPSHOT,
        V3_3_5,

        V3_3_6_SNAPSHOT,
        V3_3_6,

        V3_3_7_SNAPSHOT,
        V3_3_7,

        V3_3_8_SNAPSHOT,
        V3_3_8,

        V3_3_9_SNAPSHOT,
        V3_3_9,

        V3_4_1_SNAPSHOT,
        V3_4_1,

        V3_4_2_SNAPSHOT,
        V3_4_2,

        V3_4_3_SNAPSHOT,
        V3_4_3,

        V3_4_4_SNAPSHOT,
        V3_4_4,

        V3_4_5_SNAPSHOT,
        V3_4_5,

        V3_4_6_SNAPSHOT,
        V3_4_6,

        V3_4_7_SNAPSHOT,
        V3_4_7,

        V3_4_8_SNAPSHOT,
        V3_4_8,

        V3_4_9_SNAPSHOT,
        V3_4_9,
        V3_5_1_SNAPSHOT,
        V3_5_1,

        V3_5_2_SNAPSHOT,
        V3_5_2,

        V3_5_3_SNAPSHOT,
        V3_5_3,

        V3_5_4_SNAPSHOT,
        V3_5_4,

        V3_5_5_SNAPSHOT,
        V3_5_5,

        V3_5_6_SNAPSHOT,
        V3_5_6,

        V3_5_7_SNAPSHOT,
        V3_5_7,

        V3_5_8_SNAPSHOT,
        V3_5_8,

        V3_5_9_SNAPSHOT,
        V3_5_9,
    }
}
