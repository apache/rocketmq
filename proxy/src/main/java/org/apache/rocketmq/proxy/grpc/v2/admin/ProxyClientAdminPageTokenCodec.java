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
        String normalizedPublicPageToken = StringUtils.trimToNull(publicPageToken);
        if (normalizedPublicPageToken == null) {
            return null;
        }
        if (!normalizedPublicPageToken.startsWith(VERSION_1_PREFIX)) {
            if (isVersionedToken(normalizedPublicPageToken)) {
                throw new IllegalArgumentException("Invalid page token: " + normalizedPublicPageToken);
            }
            return normalizedPublicPageToken;
        }
        String encodedReadModelPageToken = normalizedPublicPageToken.substring(VERSION_1_PREFIX.length());
        if (StringUtils.isBlank(encodedReadModelPageToken)) {
            throw new IllegalArgumentException("Invalid page token: " + normalizedPublicPageToken);
        }
        try {
            String readModelPageToken = new String(
                Base64.getUrlDecoder().decode(encodedReadModelPageToken),
                StandardCharsets.UTF_8
            );
            String normalizedReadModelPageToken = StringUtils.trimToNull(readModelPageToken);
            if (normalizedReadModelPageToken == null) {
                throw new IllegalArgumentException("Invalid page token: " + normalizedPublicPageToken);
            }
            return normalizedReadModelPageToken;
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Invalid page token: " + normalizedPublicPageToken, e);
        }
    }

    public String encode(String readModelPageToken) {
        String normalizedReadModelPageToken = StringUtils.trimToNull(readModelPageToken);
        if (normalizedReadModelPageToken == null) {
            return "";
        }
        return VERSION_1_PREFIX + Base64.getUrlEncoder()
            .withoutPadding()
            .encodeToString(normalizedReadModelPageToken.getBytes(StandardCharsets.UTF_8));
    }

    private static boolean isVersionedToken(String publicPageToken) {
        int colonIndex = publicPageToken.indexOf(':');
        if (colonIndex <= 1 || publicPageToken.charAt(0) != 'v') {
            return false;
        }
        for (int i = 1; i < colonIndex; i++) {
            if (!Character.isDigit(publicPageToken.charAt(i))) {
                return false;
            }
        }
        return true;
    }
}
