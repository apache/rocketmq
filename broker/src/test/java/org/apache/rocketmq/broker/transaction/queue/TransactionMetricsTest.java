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

package org.apache.rocketmq.broker.transaction.queue;

import org.apache.rocketmq.broker.transaction.TransactionMetrics;
import org.apache.rocketmq.broker.transaction.TransactionMetrics.Metric;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

@RunWith(MockitoJUnitRunner.class)
public class TransactionMetricsTest {
    private TransactionMetrics transactionMetrics;
    private String configPath;
    private Path path;

    @Before
    public void before() throws Exception {
        configPath = createBaseDir();
        path = Paths.get(configPath);
        transactionMetrics = spy(new TransactionMetrics(configPath));
    }

    @After
    public void after() throws Exception {
        deleteFile(configPath);
        assertFalse(path.toFile().exists());
    }

    /**
     * test addAndGet method
     */
    @Test
    public void testAddAndGet() {
        String topic = "testAddAndGet";
        int value = 10;
        long result = transactionMetrics.addAndGet(topic, value);

        assert result == value;
    }

    @Test
    public void testGetTopicPair() {
        String topic = "getTopicPair";
        Metric result = transactionMetrics.getTopicPair(topic);
        assert result != null;
    }

    @Test
    public void testGetTransactionCount() {
        String topicExist = "topicExist";
        String topicNotExist = "topicNotExist";

        transactionMetrics.addAndGet(topicExist, 10);

        assert transactionMetrics.getTransactionCount(topicExist) == 10;
        assert transactionMetrics.getTransactionCount(topicNotExist) == 0;
    }


    /**
     * test clean metrics
     */
    @Test
    public void testCleanMetrics() {
        String topic = "testCleanMetrics";
        int value = 10;
        assert transactionMetrics.addAndGet(topic, value) == value;
        transactionMetrics.cleanMetrics(Collections.singleton(topic));
        assert transactionMetrics.getTransactionCount(topic) == 0;
    }

    @Test
    public void testPersist() {
        assertFalse(path.toFile().exists());
        transactionMetrics.persist();
        assertTrue(path.toFile().exists());
        verify(transactionMetrics).persist();
    }

    private String createBaseDir() {
        String baseDir = System.getProperty("java.io.tmpdir") + File.separator + "unitteststore-" + UUID.randomUUID();
        final File file = new File(baseDir);
        if (file.exists()) {
            System.exit(1);
        }
        return baseDir;
    }

    private void deleteFile(String fileName) {
        deleteFile(new File(fileName));
    }

    private void deleteFile(File file) {
        if (!file.exists()) {
            return;
        }
        if (file.isFile()) {
            file.delete();
        } else if (file.isDirectory()) {
            File[] files = file.listFiles();
            for (File file1 : files) {
                deleteFile(file1);
            }
            file.delete();
        }
    }
}
