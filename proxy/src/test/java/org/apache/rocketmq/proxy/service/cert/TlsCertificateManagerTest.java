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
package org.apache.rocketmq.proxy.service.cert;

import org.apache.rocketmq.remoting.netty.TlsSystemConfig;
import org.apache.rocketmq.srvutil.FileWatchService;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;

import java.lang.reflect.Constructor;

import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@ExtendWith(org.mockito.junit.jupiter.MockitoExtension.class)
class TlsCertificateManagerTest {

    @Mock
    private TlsCertificateManager.TlsContextReloadListener reloadListener;

    private static TlsCertificateManager manager;

    private FileWatchService.Listener innerListener;

    @BeforeAll
    static void initTlsConfig() {
        TlsSystemConfig.tlsServerCertPath      = "/tmp/server.crt";
        TlsSystemConfig.tlsServerKeyPath       = "/tmp/server.key";
        TlsSystemConfig.tlsServerTrustCertPath = "/tmp/ca.crt";

        // Obtain the singleton after the paths are set
        manager = TlsCertificateManager.getInstance();
    }

    @BeforeEach
    void setUp() throws Exception {
        // Register the external listener
        manager.registerReloadListener(reloadListener);

        Class<?> innerClazz = Class.forName(
            "org.apache.rocketmq.proxy.service.cert.TlsCertificateManager$CertKeyFileWatchListener");
        Constructor<?> ctor = innerClazz.getDeclaredConstructor(TlsCertificateManager.class);
        ctor.setAccessible(true);
        innerListener = (FileWatchService.Listener) ctor.newInstance(manager);
    }

    @AfterEach
    void tearDown() {
        // Unregister and reset between tests to avoid interference
        manager.unregisterReloadListener(reloadListener);
        reset(reloadListener);
    }

    // Trust certificate change should trigger immediate reload
    @Test
    void trustCertChanged_shouldTriggerReload() {
        innerListener.onChanged(TlsSystemConfig.tlsServerTrustCertPath);
        verify(reloadListener, times(1)).onTlsContextReload();
    }

    // Only server certificate change should NOT trigger reload
    @Test
    void certOnlyChanged_shouldNotTriggerReload() {
        innerListener.onChanged(TlsSystemConfig.tlsServerCertPath);
        verify(reloadListener, never()).onTlsContextReload();
    }

    // Only private-key change should NOT trigger reload
    @Test
    void keyOnlyChanged_shouldNotTriggerReload() {
        innerListener.onChanged(TlsSystemConfig.tlsServerKeyPath);
        verify(reloadListener, never()).onTlsContextReload();
    }

    // Server certificate + key both changed -> trigger one reload
    @Test
    void certAndKeyChanged_shouldTriggerReloadOnce() {
        innerListener.onChanged(TlsSystemConfig.tlsServerCertPath);
        innerListener.onChanged(TlsSystemConfig.tlsServerKeyPath);
        verify(reloadListener, times(1)).onTlsContextReload();
    }
}
