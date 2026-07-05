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

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import org.apache.commons.lang3.StringUtils;

public final class ProxyClientAdminPageTokenCodec {
    private static final ProxyClientAdminPageTokenCodec INSTANCE = new ProxyClientAdminPageTokenCodec();
    private static final String VERSION_1_PREFIX = "v1:";

    private ProxyClientAdminPageTokenCodec() {
    }

    public static ProxyClientAdminPageTokenCodec getInstance() {
        return INSTANCE;
    }

    public String decode(String publicPageToken) {
        if (StringUtils.isBlank(publicPageToken)) {
            return null;
        }
        if (!publicPageToken.startsWith(VERSION_1_PREFIX)) {
            return publicPageToken;
        }
        String encodedReadModelPageToken = publicPageToken.substring(VERSION_1_PREFIX.length());
        if (StringUtils.isBlank(encodedReadModelPageToken)) {
            throw new IllegalArgumentException("Invalid page token: " + publicPageToken);
        }
        try {
            String readModelPageToken = new String(
                Base64.getUrlDecoder().decode(encodedReadModelPageToken),
                StandardCharsets.UTF_8
            );
            if (StringUtils.isBlank(readModelPageToken)) {
                throw new IllegalArgumentException("Invalid page token: " + publicPageToken);
            }
            return readModelPageToken;
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Invalid page token: " + publicPageToken, e);
        }
    }

    public String encode(String readModelPageToken) {
        if (StringUtils.isBlank(readModelPageToken)) {
            return "";
        }
        return VERSION_1_PREFIX + Base64.getUrlEncoder()
            .withoutPadding()
            .encodeToString(readModelPageToken.getBytes(StandardCharsets.UTF_8));
    }
}
