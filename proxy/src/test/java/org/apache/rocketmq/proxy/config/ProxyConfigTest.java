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

package org.apache.rocketmq.proxy.config;

import org.apache.commons.lang3.StringUtils;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class ProxyConfigTest {

    @Test
    public void initDataShouldResolveBlankLocalServeAddr() {
        ProxyConfig proxyConfig = new ProxyConfig();
        proxyConfig.setLocalServeAddr(" \t ");
        proxyConfig.setRemotingAccessAddr(" ");

        proxyConfig.initData();

        assertThat(StringUtils.isBlank(proxyConfig.getLocalServeAddr())).isFalse();
        assertThat(proxyConfig.getRemotingAccessAddr()).isEqualTo(proxyConfig.getLocalServeAddr());
    }

    @Test
    public void parseDelayLevelShouldTolerateExtraWhitespace() {
        ProxyConfig proxyConfig = new ProxyConfig();
        proxyConfig.setMessageDelayLevel(" 1s   5s\n10s\t30s ");

        proxyConfig.parseDelayLevel();

        assertThat(proxyConfig.getDelayLevelTable())
            .containsEntry(1, 1000L)
            .containsEntry(2, 5000L)
            .containsEntry(3, 10000L)
            .containsEntry(4, 30000L);
    }

    @Test
    public void parseDelayLevelShouldUseDefaultWhenConfigIsBlank() {
        ProxyConfig proxyConfig = new ProxyConfig();
        proxyConfig.setMessageDelayLevel(" \t ");

        proxyConfig.parseDelayLevel();

        assertThat(proxyConfig.getDelayLevelTable())
            .containsEntry(1, 1000L)
            .containsEntry(2, 5000L)
            .containsEntry(18, 7200000L);
    }
}
