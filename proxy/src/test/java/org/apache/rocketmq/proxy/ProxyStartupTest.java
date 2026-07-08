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

package org.apache.rocketmq.proxy;

import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import io.grpc.BindableService;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.BrokerStartup;
import org.apache.rocketmq.broker.metrics.BrokerMetricsManager;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.proxy.config.Configuration;
import org.apache.rocketmq.proxy.config.ConfigurationManager;
import org.apache.rocketmq.proxy.config.ProxyConfig;
import org.apache.rocketmq.proxy.grpc.v2.DefaultGrpcMessagingActivity;
import org.apache.rocketmq.proxy.grpc.v2.GrpcMessagingApplication;
import org.apache.rocketmq.proxy.grpc.v2.admin.ProxyClientAdminPeerGrpcService;
import org.apache.rocketmq.proxy.processor.DefaultMessagingProcessor;
import org.apache.rocketmq.proxy.processor.MessagingProcessor;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;

import static org.apache.rocketmq.proxy.config.ConfigurationManager.RMQ_PROXY_HOME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;

public class ProxyStartupTest {

    private File proxyHome;

    @Before
    public void before() throws Throwable {
        proxyHome = new File(System.getProperty("java.io.tmpdir"), UUID.randomUUID().toString().replace('-', '_'));
        if (!proxyHome.exists()) {
            proxyHome.mkdirs();
        }
        String folder = "rmq-proxy-home";
        PathMatchingResourcePatternResolver resolver = new PathMatchingResourcePatternResolver(getClass().getClassLoader());
        Resource[] resources = resolver.getResources(String.format("classpath:%s/**/*", folder));
        for (Resource resource : resources) {
            if (!resource.isReadable()) {
                continue;
            }
            String description = resource.getDescription();
            int start = description.indexOf('[');
            int end = description.lastIndexOf(']');
            String path = description.substring(start + 1, end);
            try (InputStream inputStream = resource.getInputStream()) {
                copyTo(path, inputStream, proxyHome, folder);
            }
        }
        System.setProperty(RMQ_PROXY_HOME, proxyHome.getAbsolutePath());
    }

    private void copyTo(String path, InputStream src, File dstDir, String flag) throws IOException {
        Preconditions.checkNotNull(flag);
        Iterator<String> iterator = Splitter.on(File.separatorChar).split(path).iterator();
        boolean found = false;
        File dir = dstDir;
        while (iterator.hasNext()) {
            String current = iterator.next();
            if (!found && flag.equals(current)) {
                found = true;
                continue;
            }
            if (found) {
                if (!iterator.hasNext()) {
                    dir = new File(dir, current);
                } else {
                    dir = new File(dir, current);
                    if (!dir.exists()) {
                        Assert.assertTrue(dir.mkdir());
                    }
                }
            }
        }

        Assert.assertTrue(dir.createNewFile());
        byte[] buffer = new byte[4096];
        BufferedInputStream bis = new BufferedInputStream(src);
        int len = 0;
        try (BufferedOutputStream bos = new BufferedOutputStream(Files.newOutputStream(dir.toPath()))) {
            while ((len = bis.read(buffer)) > 0) {
                bos.write(buffer, 0, len);
            }
        }
    }

    private void recursiveDelete(File file) {
        if (file.isFile()) {
            file.delete();
            return;
        }

        File[] files = file.listFiles();
        for (File f : files) {
            recursiveDelete(f);
        }
        file.delete();
    }

    @After
    public void after() {
        System.clearProperty(RMQ_PROXY_HOME);
        System.clearProperty(MixAll.NAMESRV_ADDR_PROPERTY);
        System.clearProperty(Configuration.CONFIG_PATH_PROPERTY);
        recursiveDelete(proxyHome);
    }

    @Test
    public void testParseAndInitCommandLineArgument() throws Exception {
        Path configFilePath = Files.createTempFile("testParseAndInitCommandLineArgument", ".json");
        String configData = "{}";
        Files.write(configFilePath, configData.getBytes(StandardCharsets.UTF_8));

        String brokerConfigPath = "brokerConfigPath";
        String proxyConfigPath = configFilePath.toAbsolutePath().toString();
        String proxyMode = "LOCAL";
        String namesrvAddr = "namesrvAddr";
        CommandLineArgument commandLineArgument = ProxyStartup.parseCommandLineArgument(new String[] {
            "-bc", brokerConfigPath,
            "-pc", proxyConfigPath,
            "-pm", proxyMode,
            "-n", namesrvAddr
        });

        assertEquals(brokerConfigPath, commandLineArgument.getBrokerConfigPath());
        assertEquals(proxyConfigPath, commandLineArgument.getProxyConfigPath());
        assertEquals(proxyMode, commandLineArgument.getProxyMode());
        assertEquals(namesrvAddr, commandLineArgument.getNamesrvAddr());

        ProxyStartup.initConfiguration(commandLineArgument);

        ProxyConfig config = ConfigurationManager.getProxyConfig();
        assertEquals(brokerConfigPath, config.getBrokerConfigPath());
        assertEquals(proxyMode, config.getProxyMode());
        assertEquals(namesrvAddr, config.getNamesrvAddr());
    }

    @Test
    public void testLocalModeWithNameSrvAddrByProperty() throws Exception {
        String namesrvAddr = "namesrvAddr";
        System.setProperty(MixAll.NAMESRV_ADDR_PROPERTY, namesrvAddr);
        CommandLineArgument commandLineArgument = ProxyStartup.parseCommandLineArgument(new String[] {
            "-pm", "local"
        });
        ProxyStartup.initConfiguration(commandLineArgument);

        ProxyConfig config = ConfigurationManager.getProxyConfig();
        assertEquals(namesrvAddr, config.getNamesrvAddr());

        validateBrokerCreateArgsWithNamsrvAddr(config, namesrvAddr);
    }

    private void validateBrokerCreateArgsWithNamsrvAddr(ProxyConfig config, String namesrvAddr) {
        try (MockedStatic<BrokerStartup> brokerStartupMocked = mockStatic(BrokerStartup.class);
             MockedStatic<DefaultMessagingProcessor> messagingProcessorMocked = mockStatic(DefaultMessagingProcessor.class)) {
            ArgumentCaptor<Object> args = ArgumentCaptor.forClass(Object.class);
            BrokerController brokerControllerMocked = mock(BrokerController.class);
            BrokerMetricsManager brokerMetricsManagerMocked = mock(BrokerMetricsManager.class);
            Mockito.when(brokerMetricsManagerMocked.getBrokerMeter()).thenReturn(OpenTelemetrySdk.builder().build().getMeter("test"));
            Mockito.when(brokerControllerMocked.getBrokerMetricsManager()).thenReturn(brokerMetricsManagerMocked);
            brokerStartupMocked.when(() -> BrokerStartup.createBrokerController((String[]) args.capture()))
                .thenReturn(brokerControllerMocked);
            messagingProcessorMocked.when(() -> DefaultMessagingProcessor.createForLocalMode(any(), any()))
                .thenReturn(mock(DefaultMessagingProcessor.class));

            ProxyStartup.createMessagingProcessor();
            String[] passArgs = (String[]) args.getValue();
            assertEquals("-c", passArgs[0]);
            assertEquals(config.getBrokerConfigPath(), passArgs[1]);
            assertEquals("-n", passArgs[2]);
            assertEquals(namesrvAddr, passArgs[3]);
            assertEquals(4, passArgs.length);
        }
    }

    @Test
    public void testLocalModeWithNameSrvAddrByConfigFile() throws Exception {
        String namesrvAddr = "namesrvAddr";
        System.setProperty(MixAll.NAMESRV_ADDR_PROPERTY, "foo");
        Path configFilePath = Files.createTempFile("testLocalModeWithNameSrvAddrByConfigFile", ".json");
        String configData = "{\n" +
            "  \"namesrvAddr\": \"namesrvAddr\"\n" +
            "}";
        Files.write(configFilePath, configData.getBytes(StandardCharsets.UTF_8));

        CommandLineArgument commandLineArgument = ProxyStartup.parseCommandLineArgument(new String[] {
            "-pm", "local",
            "-pc", configFilePath.toAbsolutePath().toString()
        });
        ProxyStartup.initConfiguration(commandLineArgument);

        ProxyConfig config = ConfigurationManager.getProxyConfig();
        assertEquals(namesrvAddr, config.getNamesrvAddr());

        validateBrokerCreateArgsWithNamsrvAddr(config, namesrvAddr);
    }

    @Test
    public void testLocalModeWithNameSrvAddrByCommandLine() throws Exception {
        String namesrvAddr = "namesrvAddr";
        System.setProperty(MixAll.NAMESRV_ADDR_PROPERTY, "foo");
        Path configFilePath = Files.createTempFile("testLocalModeWithNameSrvAddrByCommandLine", ".json");
        String configData = "{\n" +
            "  \"namesrvAddr\": \"foo\"\n" +
            "}";
        Files.write(configFilePath, configData.getBytes(StandardCharsets.UTF_8));

        CommandLineArgument commandLineArgument = ProxyStartup.parseCommandLineArgument(new String[] {
            "-pm", "local",
            "-pc", configFilePath.toAbsolutePath().toString(),
            "-n", namesrvAddr
        });
        ProxyStartup.initConfiguration(commandLineArgument);

        ProxyConfig config = ConfigurationManager.getProxyConfig();
        assertEquals(namesrvAddr, config.getNamesrvAddr());

        validateBrokerCreateArgsWithNamsrvAddr(config, namesrvAddr);
    }

    @Test
    public void testLocalModeWithAllArgs() throws Exception {
        String namesrvAddr = "namesrvAddr";
        System.setProperty(MixAll.NAMESRV_ADDR_PROPERTY, "foo");
        Path configFilePath = Files.createTempFile("testLocalMode", ".json");
        String configData = "{\n" +
            "  \"namesrvAddr\": \"foo\"\n" +
            "}";
        Files.write(configFilePath, configData.getBytes(StandardCharsets.UTF_8));
        Path brokerConfigFilePath = Files.createTempFile("testLocalModeBrokerConfig", ".json");

        CommandLineArgument commandLineArgument = ProxyStartup.parseCommandLineArgument(new String[] {
            "-pm", "local",
            "-pc", configFilePath.toAbsolutePath().toString(),
            "-n", namesrvAddr,
            "-bc", brokerConfigFilePath.toAbsolutePath().toString()
        });
        ProxyStartup.initConfiguration(commandLineArgument);

        ProxyConfig config = ConfigurationManager.getProxyConfig();
        assertEquals(namesrvAddr, config.getNamesrvAddr());
        assertEquals(brokerConfigFilePath.toAbsolutePath().toString(), config.getBrokerConfigPath());

        validateBrokerCreateArgsWithNamsrvAddr(config, namesrvAddr);
    }

    @Test
    public void testClusterMode() throws Exception {
        CommandLineArgument commandLineArgument = ProxyStartup.parseCommandLineArgument(new String[] {
            "-pm", "cluster"
        });
        ProxyStartup.initConfiguration(commandLineArgument);

        try (MockedStatic<DefaultMessagingProcessor> messagingProcessorMocked = mockStatic(DefaultMessagingProcessor.class)) {
            DefaultMessagingProcessor processor = mock(DefaultMessagingProcessor.class);
            messagingProcessorMocked.when(DefaultMessagingProcessor::createForClusterMode)
                .thenReturn(processor);

            assertSame(processor, ProxyStartup.createMessagingProcessor());
        }
    }

    @Test
    public void testCreateServiceProcessorUsesSuppliedSharedActivity() throws Exception {
        CommandLineArgument commandLineArgument = ProxyStartup.parseCommandLineArgument(new String[] {
            "-pm", "cluster"
        });
        ProxyStartup.initConfiguration(commandLineArgument);
        MessagingProcessor messagingProcessor = mock(MessagingProcessor.class);
        DefaultGrpcMessagingActivity sharedActivity = mock(DefaultGrpcMessagingActivity.class);

        GrpcMessagingApplication application = ProxyStartup.createServiceProcessor(messagingProcessor, sharedActivity);

        Field activityField = GrpcMessagingApplication.class.getDeclaredField("grpcMessagingActivity");
        activityField.setAccessible(true);
        assertSame(sharedActivity, activityField.get(application));
    }

    @Test
    public void testCreateGrpcBindableServicesUsesSuppliedSharedActivity() throws Exception {
        CommandLineArgument commandLineArgument = ProxyStartup.parseCommandLineArgument(new String[] {
            "-pm", "cluster"
        });
        ProxyStartup.initConfiguration(commandLineArgument);
        MessagingProcessor messagingProcessor = mock(MessagingProcessor.class);
        DefaultGrpcMessagingActivity sharedActivity = mock(DefaultGrpcMessagingActivity.class);

        List<BindableService> services = ProxyStartup.createGrpcBindableServices(messagingProcessor, sharedActivity);

        assertEquals(1, services.size());
        Assert.assertTrue(services.get(0) instanceof GrpcMessagingApplication);
        Field activityField = GrpcMessagingApplication.class.getDeclaredField("grpcMessagingActivity");
        activityField.setAccessible(true);
        assertSame(sharedActivity, activityField.get(services.get(0)));
    }

    @Test
    public void testProxyAdminGrpcConfigDefaultsToDisabledIndependentPort() {
        ProxyConfig config = new ProxyConfig();

        Assert.assertFalse(config.isEnableProxyAdminGrpcServer());
        assertEquals(Integer.valueOf(8082), config.getProxyAdminGrpcServerPort());
        assertEquals(4, config.getProxyAdminGrpcThreadPoolNums());
        assertEquals(10000, config.getProxyAdminGrpcThreadPoolQueueCapacity());
    }

    @Test
    public void testCreateGrpcBindableServicesDoesNotRegisterAdminPeerService() throws Exception {
        CommandLineArgument commandLineArgument = ProxyStartup.parseCommandLineArgument(new String[] {
            "-pm", "cluster"
        });
        ProxyStartup.initConfiguration(commandLineArgument);
        MessagingProcessor messagingProcessor = mock(MessagingProcessor.class);
        DefaultGrpcMessagingActivity sharedActivity = mock(DefaultGrpcMessagingActivity.class);
        ProxyClientAdminPeerGrpcService peerGrpcService = mock(ProxyClientAdminPeerGrpcService.class);
        Mockito.when(sharedActivity.getProxyClientAdminPeerGrpcService()).thenReturn(peerGrpcService);

        List<BindableService> services = ProxyStartup.createGrpcBindableServices(messagingProcessor, sharedActivity);

        assertEquals(1, services.size());
        Assert.assertTrue(services.get(0) instanceof GrpcMessagingApplication);
    }

    @Test
    public void testCreateProxyAdminGrpcBindableServicesRegistersAdminBeforePeerService() throws Exception {
        CommandLineArgument commandLineArgument = ProxyStartup.parseCommandLineArgument(new String[] {
            "-pm", "cluster"
        });
        ProxyStartup.initConfiguration(commandLineArgument);
        DefaultGrpcMessagingActivity sharedActivity = mock(DefaultGrpcMessagingActivity.class);
        BindableService adminService = mock(BindableService.class);
        ProxyClientAdminPeerGrpcService peerGrpcService = mock(ProxyClientAdminPeerGrpcService.class);
        Mockito.when(sharedActivity.getProxyClientAdminPeerGrpcService()).thenReturn(peerGrpcService);

        List<BindableService> services = ProxyStartup.createProxyAdminGrpcBindableServices(
            sharedActivity,
            activity -> {
                assertSame(sharedActivity, activity);
                return Collections.singletonList(adminService);
            }
        );

        assertEquals(2, services.size());
        assertSame(adminService, services.get(0));
        assertSame(peerGrpcService, services.get(1));
    }

    @Test
    public void testCreateProxyAdminServerExecutorUsesIndependentThreadNameAndConfig() throws Exception {
        CommandLineArgument commandLineArgument = ProxyStartup.parseCommandLineArgument(new String[] {
            "-pm", "cluster"
        });
        ProxyStartup.initConfiguration(commandLineArgument);
        ProxyConfig config = ConfigurationManager.getProxyConfig();
        config.setProxyAdminGrpcThreadPoolNums(1);
        config.setProxyAdminGrpcThreadPoolQueueCapacity(2);

        ThreadPoolExecutor executor = ProxyStartup.createProxyAdminServerExecutor();
        try {
            String threadName = executor.submit(() -> Thread.currentThread().getName()).get(5, TimeUnit.SECONDS);

            Assert.assertTrue(threadName.startsWith("ProxyAdminGrpcRequestExecutorThread"));
            assertEquals(1, executor.getCorePoolSize());
            assertEquals(2, executor.getQueue().remainingCapacity());
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    public void testCreateGrpcBindableServicesAppendsAdditionalServicesAfterMessagingService() throws Exception {
        CommandLineArgument commandLineArgument = ProxyStartup.parseCommandLineArgument(new String[] {
            "-pm", "cluster"
        });
        ProxyStartup.initConfiguration(commandLineArgument);
        MessagingProcessor messagingProcessor = mock(MessagingProcessor.class);
        DefaultGrpcMessagingActivity sharedActivity = mock(DefaultGrpcMessagingActivity.class);
        BindableService adminService = mock(BindableService.class);

        List<BindableService> services = ProxyStartup.createGrpcBindableServices(
            messagingProcessor,
            sharedActivity,
            Collections.singletonList(adminService)
        );

        assertEquals(2, services.size());
        Assert.assertTrue(services.get(0) instanceof GrpcMessagingApplication);
        assertSame(adminService, services.get(1));
        Field activityField = GrpcMessagingApplication.class.getDeclaredField("grpcMessagingActivity");
        activityField.setAccessible(true);
        assertSame(sharedActivity, activityField.get(services.get(0)));
    }

    @Test
    public void testCreateProxyAdminGrpcBindableServicesUsesAdminServiceFactoryWithSharedActivity() throws Exception {
        CommandLineArgument commandLineArgument = ProxyStartup.parseCommandLineArgument(new String[] {
            "-pm", "cluster"
        });
        ProxyStartup.initConfiguration(commandLineArgument);
        DefaultGrpcMessagingActivity sharedActivity = mock(DefaultGrpcMessagingActivity.class);
        BindableService adminService = mock(BindableService.class);

        List<BindableService> services = ProxyStartup.createProxyAdminGrpcBindableServices(
            sharedActivity,
            activity -> {
                assertSame(sharedActivity, activity);
                return Collections.singletonList(adminService);
            }
        );

        assertEquals(1, services.size());
        assertSame(adminService, services.get(0));
    }

    @Test
    public void testCreateGrpcBindableServicesRejectsNullAdditionalService() throws Exception {
        CommandLineArgument commandLineArgument = ProxyStartup.parseCommandLineArgument(new String[] {
            "-pm", "cluster"
        });
        ProxyStartup.initConfiguration(commandLineArgument);
        MessagingProcessor messagingProcessor = mock(MessagingProcessor.class);
        DefaultGrpcMessagingActivity sharedActivity = mock(DefaultGrpcMessagingActivity.class);

        IllegalArgumentException exception = Assert.assertThrows(
            IllegalArgumentException.class,
            () -> ProxyStartup.createGrpcBindableServices(
                messagingProcessor,
                sharedActivity,
                Collections.singletonList(null)
            )
        );

        Assert.assertTrue(exception.getMessage().contains("additional service is required"));
    }

    @Test
    public void testCreateProxyAdminGrpcBindableServicesRejectsNullAdminServiceFactory()
        throws Exception {
        CommandLineArgument commandLineArgument = ProxyStartup.parseCommandLineArgument(new String[] {
            "-pm", "cluster"
        });
        ProxyStartup.initConfiguration(commandLineArgument);
        DefaultGrpcMessagingActivity sharedActivity = mock(DefaultGrpcMessagingActivity.class);
        int lifecycleCountBefore = proxyLifecycleComponentCount();

        IllegalArgumentException exception = Assert.assertThrows(
            IllegalArgumentException.class,
            () -> ProxyStartup.createProxyAdminGrpcBindableServices(
                sharedActivity,
                (ProxyStartup.ProxyAdminServiceFactory) null
            )
        );

        Assert.assertTrue(exception.getMessage().contains("proxy admin service factory is required"));
        assertEquals(lifecycleCountBefore, proxyLifecycleComponentCount());
    }

    @Test
    public void testCreateProxyAdminGrpcBindableServicesRejectsNullSharedActivityBeforeAdminFactory() throws Exception {
        CommandLineArgument commandLineArgument = ProxyStartup.parseCommandLineArgument(new String[] {
            "-pm", "cluster"
        });
        ProxyStartup.initConfiguration(commandLineArgument);
        AtomicBoolean factoryInvoked = new AtomicBoolean(false);
        int lifecycleCountBefore = proxyLifecycleComponentCount();

        IllegalArgumentException exception = Assert.assertThrows(
            IllegalArgumentException.class,
            () -> ProxyStartup.createProxyAdminGrpcBindableServices(
                null,
                activity -> {
                    factoryInvoked.set(true);
                    return Collections.emptyList();
                }
            )
        );

        Assert.assertTrue(exception.getMessage().contains("grpcMessagingActivity is required"));
        Assert.assertFalse(factoryInvoked.get());
        assertEquals(lifecycleCountBefore, proxyLifecycleComponentCount());
    }

    @Test
    public void testCreateGrpcBindableServicesRejectsNullSharedActivityBeforeAdditionalServices()
        throws Exception {
        CommandLineArgument commandLineArgument = ProxyStartup.parseCommandLineArgument(new String[] {
            "-pm", "cluster"
        });
        ProxyStartup.initConfiguration(commandLineArgument);
        MessagingProcessor messagingProcessor = mock(MessagingProcessor.class);
        BindableService additionalService = mock(BindableService.class);
        int lifecycleCountBefore = proxyLifecycleComponentCount();

        IllegalArgumentException exception = Assert.assertThrows(
            IllegalArgumentException.class,
            () -> ProxyStartup.createGrpcBindableServices(
                messagingProcessor,
                null,
                Collections.singletonList(additionalService)
            )
        );

        Assert.assertTrue(exception.getMessage().contains("grpcMessagingActivity is required"));
        assertEquals(lifecycleCountBefore, proxyLifecycleComponentCount());
    }

    @Test
    public void testCreateProxyAdminGrpcBindableServicesRejectsNullAdminService()
        throws Exception {
        CommandLineArgument commandLineArgument = ProxyStartup.parseCommandLineArgument(new String[] {
            "-pm", "cluster"
        });
        ProxyStartup.initConfiguration(commandLineArgument);
        DefaultGrpcMessagingActivity sharedActivity = mock(DefaultGrpcMessagingActivity.class);
        int lifecycleCountBefore = proxyLifecycleComponentCount();

        IllegalArgumentException exception = Assert.assertThrows(
            IllegalArgumentException.class,
            () -> ProxyStartup.createProxyAdminGrpcBindableServices(
                sharedActivity,
                activity -> Collections.singletonList(null)
            )
        );

        Assert.assertTrue(exception.getMessage().contains("proxy admin service is required"));
        assertEquals(lifecycleCountBefore, proxyLifecycleComponentCount());
    }

    @Test
    public void testCreateProxyAdminGrpcBindableServicesRejectsPeerGrpcServiceFailure()
        throws Exception {
        CommandLineArgument commandLineArgument = ProxyStartup.parseCommandLineArgument(new String[] {
            "-pm", "cluster"
        });
        ProxyStartup.initConfiguration(commandLineArgument);
        DefaultGrpcMessagingActivity sharedActivity = mock(DefaultGrpcMessagingActivity.class);
        Mockito.when(sharedActivity.getProxyClientAdminPeerGrpcService())
            .thenThrow(new IllegalStateException("peer grpc service is unavailable"));
        int lifecycleCountBefore = proxyLifecycleComponentCount();

        IllegalStateException exception = Assert.assertThrows(
            IllegalStateException.class,
            () -> ProxyStartup.createProxyAdminGrpcBindableServices(
                sharedActivity,
                activity -> Collections.emptyList()
            )
        );

        Assert.assertTrue(exception.getMessage().contains("peer grpc service is unavailable"));
        assertEquals(lifecycleCountBefore, proxyLifecycleComponentCount());
    }

    @Test
    public void testCreateProxyAdminGrpcBindableServicesAppendsAdminServiceBeforePeerGrpcService() throws Exception {
        CommandLineArgument commandLineArgument = ProxyStartup.parseCommandLineArgument(new String[] {
            "-pm", "cluster"
        });
        ProxyStartup.initConfiguration(commandLineArgument);
        DefaultGrpcMessagingActivity sharedActivity = mock(DefaultGrpcMessagingActivity.class);
        BindableService adminService = mock(BindableService.class);
        ProxyClientAdminPeerGrpcService peerGrpcService = mock(ProxyClientAdminPeerGrpcService.class);
        Mockito.when(sharedActivity.getProxyClientAdminPeerGrpcService()).thenReturn(peerGrpcService);

        List<BindableService> services = ProxyStartup.createProxyAdminGrpcBindableServices(
            sharedActivity,
            activity -> Collections.singletonList(adminService)
        );

        assertEquals(2, services.size());
        assertSame(adminService, services.get(0));
        assertSame(peerGrpcService, services.get(1));
    }

    @Test
    public void testCreateGrpcBindableServicesDoesNotAppendPeerGrpcServiceBeforeAdditionalServices() throws Exception {
        CommandLineArgument commandLineArgument = ProxyStartup.parseCommandLineArgument(new String[] {
            "-pm", "cluster"
        });
        ProxyStartup.initConfiguration(commandLineArgument);
        MessagingProcessor messagingProcessor = mock(MessagingProcessor.class);
        DefaultGrpcMessagingActivity sharedActivity = mock(DefaultGrpcMessagingActivity.class);
        ProxyClientAdminPeerGrpcService peerGrpcService = mock(ProxyClientAdminPeerGrpcService.class);
        BindableService additionalService = mock(BindableService.class);
        Mockito.when(sharedActivity.getProxyClientAdminPeerGrpcService()).thenReturn(peerGrpcService);

        List<BindableService> services = ProxyStartup.createGrpcBindableServices(
            messagingProcessor,
            sharedActivity,
            Collections.singletonList(additionalService)
        );

        assertEquals(2, services.size());
        Assert.assertTrue(services.get(0) instanceof GrpcMessagingApplication);
        assertSame(additionalService, services.get(1));
    }

    @Test
    public void testCreateProxyAdminGrpcBindableServicesRejectsPeerGrpcServiceFailureAfterAdminServices()
        throws Exception {
        CommandLineArgument commandLineArgument = ProxyStartup.parseCommandLineArgument(new String[] {
            "-pm", "cluster"
        });
        ProxyStartup.initConfiguration(commandLineArgument);
        DefaultGrpcMessagingActivity sharedActivity = mock(DefaultGrpcMessagingActivity.class);
        BindableService additionalService = mock(BindableService.class);
        Mockito.when(sharedActivity.getProxyClientAdminPeerGrpcService())
            .thenThrow(new IllegalStateException("peer grpc service is unavailable"));
        int lifecycleCountBefore = proxyLifecycleComponentCount();

        IllegalStateException exception = Assert.assertThrows(
            IllegalStateException.class,
            () -> ProxyStartup.createProxyAdminGrpcBindableServices(
                sharedActivity,
                activity -> Collections.singletonList(additionalService)
            )
        );

        Assert.assertTrue(exception.getMessage().contains("peer grpc service is unavailable"));
        assertEquals(lifecycleCountBefore, proxyLifecycleComponentCount());
    }

    private static int proxyLifecycleComponentCount() throws Exception {
        Field proxyStartAndShutdownField = ProxyStartup.class.getDeclaredField("PROXY_START_AND_SHUTDOWN");
        proxyStartAndShutdownField.setAccessible(true);
        Object proxyStartAndShutdown = proxyStartAndShutdownField.get(null);
        Field startAndShutdownListField =
            proxyStartAndShutdown.getClass().getSuperclass().getDeclaredField("startAndShutdownList");
        startAndShutdownListField.setAccessible(true);
        List<?> startAndShutdownList = (List<?>) startAndShutdownListField.get(proxyStartAndShutdown);
        return startAndShutdownList.size();
    }
}
