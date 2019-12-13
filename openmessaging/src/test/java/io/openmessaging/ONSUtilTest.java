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

package io.openmessaging;

import java.util.Properties;
import org.apache.rocketmq.ons.api.impl.rocketmq.ONSUtil;

import static org.assertj.core.api.Assertions.assertThat;

public class ONSUtilTest {
    @org.junit.Test
    public void extractProperties() throws Exception {
        Properties properties = new Properties();
        properties.put("Integer", 123);

        assertThat(properties.get("Integer")).isEqualTo(123);
        assertThat(properties.getProperty("Integer")).isNull();

        Properties newPro = ONSUtil.extractProperties(properties);
        assertThat(newPro.getProperty("Integer")).isEqualTo("123");
        assertThat(newPro.get("Integer")).isEqualTo("123");
    }

    @org.junit.Test
    public void extractProperties_WithInner() throws Exception {
        Properties inner = new Properties();
        inner.put("Integer", 123);

        Properties properties = new Properties(inner);

        assertThat(properties.get("Integer")).isNull();
        assertThat(properties.getProperty("Integer")).isNull();

        inner.put("String", "String");
        assertThat(properties.get("String")).isNull();
        assertThat(properties.getProperty("String")).isEqualTo("String");


        Properties newPro = ONSUtil.extractProperties(properties);
        assertThat(newPro.getProperty("Integer")).isEqualTo("123");
        assertThat(newPro.get("Integer")).isEqualTo("123");
        assertThat(newPro.getProperty("String")).isEqualTo("String");
        assertThat(newPro.get("String")).isEqualTo("String");
    }
}