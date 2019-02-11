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
package org.apache.rocketmq.snode.session;

import java.util.concurrent.ConcurrentHashMap;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.snode.SnodeController;

public class SessionManagerImpl {

    private static final InternalLogger log = InternalLoggerFactory
            .getLogger(LoggerName.SNODE_LOGGER_NAME);

    private final ConcurrentHashMap<String/*clientId*/, Session> clientSessionTable = new ConcurrentHashMap<>(
            1024);

    private final SnodeController snodeController;

    public SessionManagerImpl(SnodeController snodeController) {
        this.snodeController = snodeController;
    }

    public boolean register(String clientId, Session session) {
        boolean updated = false;
        if (clientId != null && session != null) {
            Session prev = clientSessionTable.put(clientId, session);
            if (prev != null) {
                log.info("Session updated, clientId: {} session: {}", clientId,
                        session);
                updated = true;
            } else {
                log.info("New session registered, clientId: {} session: {}", clientId,
                        session);
            }
            session.setLastUpdateTimestamp(System.currentTimeMillis());
        }
        return updated;
    }

    public void unRegister(String clientId) {
        Session prev = clientSessionTable.remove(clientId);
        if (prev != null) {
            log.info("Unregister session: {}  of  client, {}", prev, clientId);
        }
    }

    public Session getSession(String clientId) {
        return clientSessionTable.get(clientId);
    }

    public SnodeController getSnodeController() {
        return snodeController;
    }
}
