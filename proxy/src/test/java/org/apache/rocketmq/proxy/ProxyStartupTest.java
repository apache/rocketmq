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
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.UUID;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.proxy.config.Configuration;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;

import static org.apache.rocketmq.proxy.config.ConfigurationManager.RMQ_PROXY_HOME;
import static org.junit.Assert.assertEquals;

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

    }

    @Test
    public void testClusterMode() throws Exception {
        CommandLineArgument commandLineArgument = ProxyStartup.parseCommandLineArgument(new String[] {
            "-pm", "cluster"
        });
        assertEquals("cluster", commandLineArgument.getProxyMode());
    }
}
