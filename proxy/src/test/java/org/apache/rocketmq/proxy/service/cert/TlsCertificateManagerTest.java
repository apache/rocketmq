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

import java.io.FileWriter;
import org.apache.rocketmq.proxy.config.ConfigurationManager;
import org.apache.rocketmq.proxy.config.ProxyConfig;
import org.apache.rocketmq.remoting.netty.TlsSystemConfig;
import org.apache.rocketmq.srvutil.FileWatchService;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.junit.jupiter.MockitoExtension;
import org.powermock.reflect.Whitebox;

import java.io.File;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
public class TlsCertificateManagerTest {

    @TempDir
    Path tempDir;

    private TlsCertificateManager manager;

    @Mock
    private ProxyConfig proxyConfig;

    @Mock
    private TlsCertificateManager.TlsContextReloadListener listener1;

    @Mock
    private TlsCertificateManager.TlsContextReloadListener listener2;

    private File certFile;
    private File keyFile;
    private FileWatchService.Listener fileWatchListener;
    private Field configField;
    private ProxyConfig originalConfig;

    @BeforeAll
    public static void setUpAll() throws Exception {
        ConfigurationManager.initEnv();
        ConfigurationManager.intConfig();
    }

    @BeforeEach
    public void setUp() throws Exception {
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

    @AfterEach
    public void tearDown() throws Exception {
        // Restore the original config
        if (configField != null && originalConfig != null) {
            configField.set(null, originalConfig);
        }
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
        // Create a spy to verify the internal fileWatchService methods are called
        TlsCertificateManager managerSpy = spy(manager);

        // Get access to the internal fileWatchService
        Field watchServiceField = TlsCertificateManager.class.getDeclaredField("fileWatchService");
        watchServiceField.setAccessible(true);
        FileWatchService watchService = (FileWatchService) watchServiceField.get(managerSpy);
        FileWatchService watchServiceSpy = spy(watchService);
        watchServiceField.set(managerSpy, watchServiceSpy);

        // Test start method
        managerSpy.start();
        verify(watchServiceSpy).start();

        // Test shutdown method
        managerSpy.shutdown();
        verify(watchServiceSpy).shutdown();
    }

    @Test
    public void testRegisterAndUnregisterListener() {
        // Test registering a listener
        manager.registerReloadListener(listener1);

        List<TlsCertificateManager.TlsContextReloadListener> listeners = manager.getReloadListeners();
        assertEquals(1, listeners.size());
        assertTrue(listeners.contains(listener1));

        // Test registering another listener
        manager.registerReloadListener(listener2);
        assertEquals(2, listeners.size());
        assertTrue(listeners.contains(listener2));

        // Test unregistering a listener
        manager.unregisterReloadListener(listener1);
        assertEquals(1, listeners.size());
        assertFalse(listeners.contains(listener1));
        assertTrue(listeners.contains(listener2));

        // Test handling null listeners
        manager.registerReloadListener(null);
        assertEquals(1, listeners.size()); // Should remain unchanged

        manager.unregisterReloadListener(null);
        assertEquals(1, listeners.size()); // Should remain unchanged
    }

    @Test
    public void testFileChangeNotification_CertOnly() throws Exception {
        // Setup test
        manager.registerReloadListener(listener1);

        // Trigger cert file change only
        fileWatchListener.onChanged(certFile.getAbsolutePath());

        // Verify listener not called yet
        verify(listener1, never()).onTlsContextReload();
    }

    @Test
    public void testFileChangeNotification_KeyOnly() throws Exception {
        // Setup test
        manager.registerReloadListener(listener1);

        // Trigger key file change only
        fileWatchListener.onChanged(keyFile.getAbsolutePath());

        // Verify listener not called yet
        verify(listener1, never()).onTlsContextReload();
    }

    @Test
    public void testFileChangeNotification_BothFiles() throws Exception {
        // Setup test
        manager.registerReloadListener(listener1);

        // Trigger both file changes
        fileWatchListener.onChanged(certFile.getAbsolutePath());
        fileWatchListener.onChanged(keyFile.getAbsolutePath());

        // Verify listener is called
        verify(listener1, times(1)).onTlsContextReload();
    }

    @Test
    public void testFileChangeNotification_MultipleListeners() throws Exception {
        // Setup test
        manager.registerReloadListener(listener1);
        manager.registerReloadListener(listener2);

        // Trigger both file changes
        fileWatchListener.onChanged(certFile.getAbsolutePath());
        fileWatchListener.onChanged(keyFile.getAbsolutePath());

        // Verify both listeners are called
        verify(listener1, times(1)).onTlsContextReload();
        verify(listener2, times(1)).onTlsContextReload();
    }

    @Test
    public void testFileChangeNotification_BothFilesReverseOrder() throws Exception {
        // Setup test
        manager.registerReloadListener(listener1);

        // Trigger both file changes in reverse order
        fileWatchListener.onChanged(keyFile.getAbsolutePath());
        fileWatchListener.onChanged(certFile.getAbsolutePath());

        // Verify listener is called
        verify(listener1, times(1)).onTlsContextReload();
    }

    @Test
    public void testFileChangeNotification_RepeatedChanges() throws Exception {
        // Setup test
        manager.registerReloadListener(listener1);

        // First batch of changes
        fileWatchListener.onChanged(certFile.getAbsolutePath());
        fileWatchListener.onChanged(keyFile.getAbsolutePath());

        // Verify listener is called once
        verify(listener1, times(1)).onTlsContextReload();

        // Second batch of changes
        fileWatchListener.onChanged(certFile.getAbsolutePath());
        fileWatchListener.onChanged(keyFile.getAbsolutePath());

        // Verify listener is called again (total twice)
        verify(listener1, times(2)).onTlsContextReload();
    }

    @Test
    public void testFileChangeNotification_UnknownFile() throws Exception {
        // Setup test
        manager.registerReloadListener(listener1);

        // Trigger change to an unknown file
        fileWatchListener.onChanged("/unknown/file/path");

        // Verify listener is not called
        verify(listener1, never()).onTlsContextReload();
    }

    @Test
    public void testFileChangeNotification_ListenerThrowsException() throws Exception {
        // Setup a listener that throws an exception
        TlsCertificateManager.TlsContextReloadListener exceptionListener = mock(TlsCertificateManager.TlsContextReloadListener.class);
        doThrow(new RuntimeException("Test exception")).when(exceptionListener).onTlsContextReload();

        // Register both listeners
        manager.registerReloadListener(exceptionListener);
        manager.registerReloadListener(listener1);

        // Trigger both file changes
        fileWatchListener.onChanged(certFile.getAbsolutePath());
        fileWatchListener.onChanged(keyFile.getAbsolutePath());

        // Verify both listeners were called despite the exception
        verify(exceptionListener, times(1)).onTlsContextReload();
        verify(listener1, times(1)).onTlsContextReload();
    }

    @Test
    public void testInnerCertKeyFileWatchListener() throws Exception {
        // Get the CertKeyFileWatchListener class
        Class<?> innerClass = null;
        for (Class<?> clazz : TlsCertificateManager.class.getDeclaredClasses()) {
            if (clazz.getSimpleName().equals("CertKeyFileWatchListener")) {
                innerClass = clazz;
                break;
            }
        }

        assertNotNull(innerClass, "CertKeyFileWatchListener class not found");

        // Create a new instance
        Constructor<?> constructor = innerClass.getDeclaredConstructor(TlsCertificateManager.class);
        constructor.setAccessible(true);
        Object innerListener = constructor.newInstance(manager);

        // Register a mock listener to the manager
        manager.registerReloadListener(listener1);

        // Get the onChanged method
        Method onChangedMethod = innerClass.getDeclaredMethod("onChanged", String.class);
        onChangedMethod.setAccessible(true);

        // Test cert file change
        onChangedMethod.invoke(innerListener, certFile.getAbsolutePath());
        verify(listener1, never()).onTlsContextReload();

        // Test key file change - should trigger notification
        onChangedMethod.invoke(innerListener, keyFile.getAbsolutePath());
        verify(listener1, times(1)).onTlsContextReload();

        // Test reset of flags
        reset(listener1);

        // Call onChanged again for cert - should not trigger notification as flags are reset
        onChangedMethod.invoke(innerListener, certFile.getAbsolutePath());
        verify(listener1, never()).onTlsContextReload();
    }
}
