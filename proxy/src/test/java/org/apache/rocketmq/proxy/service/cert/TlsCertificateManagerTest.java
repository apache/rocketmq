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

import org.apache.rocketmq.proxy.config.ConfigurationManager;
import org.apache.rocketmq.remoting.netty.TlsSystemConfig;
import org.apache.rocketmq.srvutil.FileWatchService;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.File;
import java.io.FileWriter;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@RunWith(MockitoJUnitRunner.class)
public class TlsCertificateManagerTest {

    private TlsCertificateManager manager;

    @Mock
    private TlsCertificateManager.TlsContextReloadListener listener1;

    @Mock
    private TlsCertificateManager.TlsContextReloadListener listener2;

    private File certFile;
    private File keyFile;
    private FileWatchService.Listener fileWatchListener;
    
    @Before
    public void setUp() throws Exception {
        ConfigurationManager.initEnv();
        ConfigurationManager.intConfig();
        Path tempDir = Files.createTempDirectory("tls");
        // Create temporary certificate and key files
        certFile = new File(tempDir.toFile(), "server.crt");
        keyFile = new File(tempDir.toFile(), "server.key");
        try (FileWriter certWriter = new FileWriter(certFile);
             FileWriter keyWriter = new FileWriter(keyFile)) {
            certWriter.write("test certificate content");
            keyWriter.write("test key content");
        }

        // Set TlsSystemConfig paths
        TlsSystemConfig.tlsServerCertPath = certFile.getAbsolutePath();
        TlsSystemConfig.tlsServerKeyPath = keyFile.getAbsolutePath();

        // Create the TlsCertificateManager
        manager = new TlsCertificateManager();

        // Extract the file watch listener using reflection
        fileWatchListener = extractFileWatchListener(manager);
    }

    private FileWatchService.Listener extractFileWatchListener(TlsCertificateManager manager) throws Exception {
        Field fileWatchServiceField = TlsCertificateManager.class.getDeclaredField("fileWatchService");
        fileWatchServiceField.setAccessible(true);
        FileWatchService fileWatchService = (FileWatchService) fileWatchServiceField.get(manager);

        Field listenerField = FileWatchService.class.getDeclaredField("listener");
        listenerField.setAccessible(true);
        return (FileWatchService.Listener) listenerField.get(fileWatchService);
    }

    @Test
    public void testConstructor() {
        // The constructor should initialize the FileWatchService with the correct paths
        assertNotNull(manager);
    }

    @Test
    public void testStartAndShutdown() throws Exception {
        TlsCertificateManager managerSpy = spy(manager);

        Field watchServiceField = TlsCertificateManager.class.getDeclaredField("fileWatchService");
        watchServiceField.setAccessible(true);
        FileWatchService watchService = (FileWatchService) watchServiceField.get(managerSpy);
        FileWatchService watchServiceSpy = spy(watchService);
        watchServiceField.set(managerSpy, watchServiceSpy);

        managerSpy.start();
        verify(watchServiceSpy).start();

        managerSpy.shutdown();
        verify(watchServiceSpy).shutdown();
    }

    @Test
    public void testRegisterAndUnregisterListener() {
        manager.registerReloadListener(listener1);

        List<TlsCertificateManager.TlsContextReloadListener> listeners = manager.getReloadListeners();
        assertEquals(1, listeners.size());
        assertTrue(listeners.contains(listener1));

        manager.registerReloadListener(listener2);
        assertEquals(2, listeners.size());
        assertTrue(listeners.contains(listener2));

        manager.unregisterReloadListener(listener1);
        assertEquals(1, listeners.size());
        assertFalse(listeners.contains(listener1));
        assertTrue(listeners.contains(listener2));

        manager.registerReloadListener(null);
        assertEquals(1, listeners.size()); // Should remain unchanged

        manager.unregisterReloadListener(null);
        assertEquals(1, listeners.size()); // Should remain unchanged
    }

    @Test
    public void testFileChangeNotification_CertOnly() throws Exception {
        manager.registerReloadListener(listener1);

        fileWatchListener.onChanged(certFile.getAbsolutePath());

        verify(listener1, never()).onTlsContextReload();
    }

    @Test
    public void testFileChangeNotification_KeyOnly() throws Exception {
        manager.registerReloadListener(listener1);

        fileWatchListener.onChanged(keyFile.getAbsolutePath());

        verify(listener1, never()).onTlsContextReload();
    }

    @Test
    public void testFileChangeNotification_BothFiles() throws Exception {
        manager.registerReloadListener(listener1);

        fileWatchListener.onChanged(certFile.getAbsolutePath());
        fileWatchListener.onChanged(keyFile.getAbsolutePath());

        verify(listener1, times(1)).onTlsContextReload();
    }

    @Test
    public void testFileChangeNotification_MultipleListeners() throws Exception {
        manager.registerReloadListener(listener1);
        manager.registerReloadListener(listener2);

        fileWatchListener.onChanged(certFile.getAbsolutePath());
        fileWatchListener.onChanged(keyFile.getAbsolutePath());

        verify(listener1, times(1)).onTlsContextReload();
        verify(listener2, times(1)).onTlsContextReload();
    }

    @Test
    public void testFileChangeNotification_BothFilesReverseOrder() throws Exception {
        manager.registerReloadListener(listener1);

        fileWatchListener.onChanged(keyFile.getAbsolutePath());
        fileWatchListener.onChanged(certFile.getAbsolutePath());

        verify(listener1, times(1)).onTlsContextReload();
    }

    @Test
    public void testFileChangeNotification_RepeatedChanges() throws Exception {
        manager.registerReloadListener(listener1);

        fileWatchListener.onChanged(certFile.getAbsolutePath());
        fileWatchListener.onChanged(keyFile.getAbsolutePath());

        verify(listener1, times(1)).onTlsContextReload();

        fileWatchListener.onChanged(certFile.getAbsolutePath());
        fileWatchListener.onChanged(keyFile.getAbsolutePath());

        verify(listener1, times(2)).onTlsContextReload();
    }

    @Test
    public void testFileChangeNotification_UnknownFile() throws Exception {
        manager.registerReloadListener(listener1);

        fileWatchListener.onChanged("/unknown/file/path");

        verify(listener1, never()).onTlsContextReload();
    }

    @Test
    public void testFileChangeNotification_ListenerThrowsException() throws Exception {
        TlsCertificateManager.TlsContextReloadListener exceptionListener = mock(TlsCertificateManager.TlsContextReloadListener.class);
        doThrow(new RuntimeException("Test exception")).when(exceptionListener).onTlsContextReload();

        manager.registerReloadListener(exceptionListener);
        manager.registerReloadListener(listener1);

        fileWatchListener.onChanged(certFile.getAbsolutePath());
        fileWatchListener.onChanged(keyFile.getAbsolutePath());

        verify(exceptionListener, times(1)).onTlsContextReload();
        verify(listener1, times(1)).onTlsContextReload();
    }

    @Test
    public void testInnerCertKeyFileWatchListener() throws Exception {
        Class<?> innerClass = null;
        for (Class<?> clazz : TlsCertificateManager.class.getDeclaredClasses()) {
            if (clazz.getSimpleName().equals("CertKeyFileWatchListener")) {
                innerClass = clazz;
                break;
            }
        }

        assertNotNull(innerClass, "CertKeyFileWatchListener class not found");

        Constructor<?> constructor = innerClass.getDeclaredConstructor(TlsCertificateManager.class);
        constructor.setAccessible(true);
        Object innerListener = constructor.newInstance(manager);

        manager.registerReloadListener(listener1);

        Method onChangedMethod = innerClass.getDeclaredMethod("onChanged", String.class);
        onChangedMethod.setAccessible(true);

        onChangedMethod.invoke(innerListener, certFile.getAbsolutePath());
        verify(listener1, never()).onTlsContextReload();

        onChangedMethod.invoke(innerListener, keyFile.getAbsolutePath());
        verify(listener1, times(1)).onTlsContextReload();

        reset(listener1);

        onChangedMethod.invoke(innerListener, certFile.getAbsolutePath());
        verify(listener1, never()).onTlsContextReload();
    }
}
