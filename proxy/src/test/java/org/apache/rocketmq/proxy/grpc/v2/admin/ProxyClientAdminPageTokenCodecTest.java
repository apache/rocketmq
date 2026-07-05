/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.rocketmq.proxy.grpc.v2.admin;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class ProxyClientAdminPageTokenCodecTest {

    @Test
    public void codecEncodesReadModelTokenAsVersionedOpaqueToken() {
        ProxyClientAdminPageTokenCodec codec = ProxyClientAdminPageTokenCodec.getInstance();

        assertThat(codec.encode("client-a")).isEqualTo("v1:Y2xpZW50LWE");
        assertThat(codec.decode("v1:Y2xpZW50LWE")).isEqualTo("client-a");
    }

    @Test
    public void codecDecodesLegacyBareReadModelTokens() {
        ProxyClientAdminPageTokenCodec codec = ProxyClientAdminPageTokenCodec.getInstance();

        assertThat(codec.decode("client-a")).isEqualTo("client-a");
    }

    @Test
    public void codecTrimsTokensBeforeEncodingAndDecoding() {
        ProxyClientAdminPageTokenCodec codec = ProxyClientAdminPageTokenCodec.getInstance();

        assertThat(codec.encode(" client-a ")).isEqualTo("v1:Y2xpZW50LWE");
        assertThat(codec.decode(" v1:Y2xpZW50LWE ")).isEqualTo("client-a");
        assertThat(codec.decode(" client-a ")).isEqualTo("client-a");
    }

    @Test
    public void codecRejectsMalformedVersionedTokens() {
        ProxyClientAdminPageTokenCodec codec = ProxyClientAdminPageTokenCodec.getInstance();

        assertThatThrownBy(() -> codec.decode("v1:*"))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Invalid page token");
    }

    @Test
    public void codecNormalizesBlankTokensAtAdapterBoundary() {
        ProxyClientAdminPageTokenCodec codec = ProxyClientAdminPageTokenCodec.getInstance();

        assertThat(codec.decode(null)).isNull();
        assertThat(codec.decode("")).isNull();
        assertThat(codec.decode(" ")).isNull();
        assertThat(codec.encode(null)).isEmpty();
        assertThat(codec.encode("")).isEmpty();
        assertThat(codec.encode(" ")).isEmpty();
    }
}
