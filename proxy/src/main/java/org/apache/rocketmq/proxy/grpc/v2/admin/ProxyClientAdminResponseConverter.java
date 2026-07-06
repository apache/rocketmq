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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.proxy.service.admin.client.ProxyClientInfo;
import org.apache.rocketmq.proxy.service.admin.client.ProxyClientPage;

public final class ProxyClientAdminResponseConverter {

    private ProxyClientAdminResponseConverter() {
    }

    public static ProxyClientAdminClientView toClientView(ProxyClientInfo clientInfo) {
        if (clientInfo == null) {
            throw new IllegalArgumentException("clientInfo is required");
        }
        return new ProxyClientAdminClientView(
            clientInfo.getClientId(),
            clientInfo.getClientType(),
            sorted(clientInfo.getGroups()),
            sorted(clientInfo.getTopics()),
            clientInfo.getLanguage(),
            clientInfo.getRemoteAddress(),
            clientInfo.getLocalAddress(),
            clientInfo.getClientVersion(),
            clientInfo.getProxyId(),
            clientInfo.getConnectTimeMillis(),
            clientInfo.getLastActiveTimeMillis()
        );
    }

    public static ProxyClientAdminPageView toPageView(ProxyClientPage page) {
        if (page == null) {
            throw new IllegalArgumentException("page is required");
        }
        List<ProxyClientAdminClientView> clients = new ArrayList<>(page.getClients().size());
        for (ProxyClientInfo clientInfo : page.getClients()) {
            clients.add(toClientView(clientInfo));
        }
        return new ProxyClientAdminPageView(
            clients,
            encodePageToken(page.getNextPageToken())
        );
    }

    private static String encodePageToken(String pageToken) {
        if (isCoordinatorPageToken(pageToken)) {
            return StringUtils.trim(pageToken);
        }
        return ProxyClientAdminPageTokenCodec.getInstance().encode(pageToken);
    }

    private static boolean isCoordinatorPageToken(String pageToken) {
        try {
            return ProxyClientAdminCoordinatorPageTokenCodec.getInstance().decode(pageToken) != null;
        } catch (IllegalArgumentException ignored) {
            return false;
        }
    }

    private static List<String> sorted(Set<String> values) {
        if (values == null || values.isEmpty()) {
            return Collections.emptyList();
        }
        List<String> result = new ArrayList<>(values);
        Collections.sort(result);
        return result;
    }
}
