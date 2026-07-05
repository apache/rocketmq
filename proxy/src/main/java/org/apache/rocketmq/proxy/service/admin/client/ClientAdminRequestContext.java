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
package org.apache.rocketmq.proxy.service.admin.client;

import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.auth.authentication.model.Subject;
import org.apache.rocketmq.common.constant.CommonConstants;
import org.apache.rocketmq.proxy.common.ProxyContext;

public class ClientAdminRequestContext {
    private final Subject subject;
    private final String sourceIp;

    private ClientAdminRequestContext(Subject subject, String sourceIp) {
        this.subject = subject;
        this.sourceIp = sourceIp;
    }

    public static ClientAdminRequestContext of(Subject subject, String sourceIp) {
        return new ClientAdminRequestContext(subject, sourceIp);
    }

    public static ClientAdminRequestContext from(ProxyContext proxyContext) {
        if (proxyContext == null) {
            throw new IllegalArgumentException("proxyContext is required");
        }
        return of(proxyContext.getSubject(), normalizeSourceIp(proxyContext.getRemoteAddress()));
    }

    public Subject getSubject() {
        return subject;
    }

    public String getSourceIp() {
        return sourceIp;
    }

    private static String normalizeSourceIp(String remoteAddress) {
        if (StringUtils.isBlank(remoteAddress)) {
            return "";
        }
        return StringUtils.substringBeforeLast(remoteAddress, CommonConstants.COLON);
    }
}
