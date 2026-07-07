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
    private static final int MAX_PUBLIC_PAGE_TOKEN_LENGTH = 4096;

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
        if (normalizedPublicPageToken.length() > MAX_PUBLIC_PAGE_TOKEN_LENGTH) {
            throw new IllegalArgumentException(
                "Invalid page token: length exceeds " + MAX_PUBLIC_PAGE_TOKEN_LENGTH
            );
        }
        if (isCoordinatorVersionedToken(normalizedPublicPageToken)) {
            throw new IllegalArgumentException("Invalid page token: " + normalizedPublicPageToken);
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
            if (!normalizedReadModelPageToken.equals(readModelPageToken)) {
                throw new IllegalArgumentException("Invalid page token: " + normalizedPublicPageToken);
            }
            if (isCoordinatorVersionedToken(normalizedReadModelPageToken)) {
                throw new IllegalArgumentException("Invalid page token: " + normalizedPublicPageToken);
            }
            String canonicalPublicPageToken = VERSION_1_PREFIX + encodeReadModelPageToken(readModelPageToken);
            if (!canonicalPublicPageToken.equals(normalizedPublicPageToken)) {
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
        if (isCoordinatorVersionedToken(normalizedReadModelPageToken)) {
            throw new IllegalArgumentException("Invalid page token: " + normalizedReadModelPageToken);
        }
        return validateEncodedPageToken(VERSION_1_PREFIX + encodeReadModelPageToken(normalizedReadModelPageToken));
    }

    private static String encodeReadModelPageToken(String readModelPageToken) {
        return Base64.getUrlEncoder()
            .withoutPadding()
            .encodeToString(readModelPageToken.getBytes(StandardCharsets.UTF_8));
    }

    private static String validateEncodedPageToken(String publicPageToken) {
        if (publicPageToken.length() > MAX_PUBLIC_PAGE_TOKEN_LENGTH) {
            throw new IllegalStateException(
                "Encoded page token length exceeds " + MAX_PUBLIC_PAGE_TOKEN_LENGTH
            );
        }
        return publicPageToken;
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

    private static boolean isCoordinatorVersionedToken(String publicPageToken) {
        int colonIndex = publicPageToken.indexOf(':');
        if (colonIndex <= 2 || publicPageToken.charAt(0) != 'c' || publicPageToken.charAt(1) != 'p') {
            return false;
        }
        for (int i = 2; i < colonIndex; i++) {
            if (!Character.isDigit(publicPageToken.charAt(i))) {
                return false;
            }
        }
        return true;
    }
}
