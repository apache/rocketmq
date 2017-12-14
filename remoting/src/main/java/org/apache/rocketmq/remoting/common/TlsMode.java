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

package org.apache.rocketmq.remoting.common;

/**
 * For server, three SSL modes are supported: disabled, permissive and enforcing.
 * <ol>
 *     <li><strong>disabled:</strong> SSL is not supported; any incoming SSL handshake will be rejected, causing connection closed.</li>
 *     <li><strong>permissive:</strong> SSL is optional, aka, server in this mode can serve client connections with or without SSL;</li>
 *     <li><strong>enforcing:</strong> SSL is required, aka, non SSL connection will be rejected.</li>
 * </ol>
 */
public enum TlsMode {

    DISABLED("disabled"),
    PERMISSIVE("permissive"),
    ENFORCING("enforcing");

    private String name;

    TlsMode(String name) {
        this.name = name;
    }

    public static TlsMode parse(String mode) {
        for (TlsMode tlsMode : TlsMode.values()) {
            if (tlsMode.name.equals(mode)) {
                return tlsMode;
            }
        }

        return PERMISSIVE;
    }

    public String getName() {
        return name;
    }
}
