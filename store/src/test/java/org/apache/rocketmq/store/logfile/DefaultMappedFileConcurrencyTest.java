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
package org.apache.rocketmq.store.logfile;

import java.io.File;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.rocketmq.common.UtilAll;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class DefaultMappedFileConcurrencyTest {

    private String storePath;
    private String fileName;
    private int fileSize = 1024 * 1024; // 1MB
    private static final int THREAD_COUNT = 10;
    private static final int OPERATIONS_PER_THREAD = 100;

    @Before
    public void setUp() throws Exception {
        storePath = System.getProperty("user.home") + File.separator + "unitteststore" + System.currentTimeMillis();
        fileName = storePath + File.separator + "00000000000000000000";
        UtilAll.ensureDirOK(storePath);
    }

    @After
    public void tearDown() throws Exception {
        UtilAll.deleteFile(new File(storePath));
    }

    @Test
    public void testConcurrentWriteWithoutMmap() throws Exception {
        DefaultMappedFile mappedFile = new DefaultMappedFile(fileName, fileSize, true);
        
        try {
            ExecutorService executor = Executors.newFixedThreadPool(THREAD_COUNT);
            CountDownLatch latch = new CountDownLatch(THREAD_COUNT);
            AtomicInteger successCount = new AtomicInteger(0);
            AtomicInteger errorCount = new AtomicInteger(0);
            
            for (int i = 0; i < THREAD_COUNT; i++) {
                final int threadId = i;
                executor.submit(() -> {
                    try {
                        for (int j = 0; j < OPERATIONS_PER_THREAD; j++) {
                            String data = String.format("Thread-%d-Operation-%d", threadId, j);
                            byte[] bytes = data.getBytes();
                            
                            boolean result = mappedFile.appendMessage(bytes);
                            if (result) {
                                successCount.incrementAndGet();
                            } else {
                                errorCount.incrementAndGet();
                            }
                        }
                    } catch (Exception e) {
                        errorCount.incrementAndGet();
                        e.printStackTrace();
                    } finally {
                        latch.countDown();
                    }
                });
            }
            
            latch.await();
            executor.shutdown();
            
            // Success count: successCount.get()
            // Error count: errorCount.get()
            // Final wrote position: mappedFile.getWrotePosition()
            
            // All operations should succeed
            Assert.assertEquals("All write operations should succeed", 
                THREAD_COUNT * OPERATIONS_PER_THREAD, successCount.get());
            Assert.assertEquals("No errors should occur", 0, errorCount.get());
            
        } finally {
            mappedFile.destroy(0);
        }
    }

    @Test
    public void testConcurrentWriteWithMmap() throws Exception {
        DefaultMappedFile mappedFile = new DefaultMappedFile(fileName, fileSize, false);
        
        try {
            ExecutorService executor = Executors.newFixedThreadPool(THREAD_COUNT);
            CountDownLatch latch = new CountDownLatch(THREAD_COUNT);
            AtomicInteger successCount = new AtomicInteger(0);
            AtomicInteger errorCount = new AtomicInteger(0);
            
            for (int i = 0; i < THREAD_COUNT; i++) {
                final int threadId = i;
                executor.submit(() -> {
                    try {
                        for (int j = 0; j < OPERATIONS_PER_THREAD; j++) {
                            String data = String.format("Thread-%d-Operation-%d", threadId, j);
                            byte[] bytes = data.getBytes();
                            
                            boolean result = mappedFile.appendMessage(bytes);
                            if (result) {
                                successCount.incrementAndGet();
                            } else {
                                errorCount.incrementAndGet();
                            }
                        }
                    } catch (Exception e) {
                        errorCount.incrementAndGet();
                        e.printStackTrace();
                    } finally {
                        latch.countDown();
                    }
                });
            }
            
            latch.await();
            executor.shutdown();
            
            // Success count: successCount.get()
            // Error count: errorCount.get()
            // Final wrote position: mappedFile.getWrotePosition()
            
            // All operations should succeed
            Assert.assertEquals("All write operations should succeed", 
                THREAD_COUNT * OPERATIONS_PER_THREAD, successCount.get());
            Assert.assertEquals("No errors should occur", 0, errorCount.get());
            
        } finally {
            mappedFile.destroy(0);
        }
    }

    @Test
    public void testConcurrentFlush() throws Exception {
        DefaultMappedFile mappedFile = new DefaultMappedFile(fileName, fileSize, true);
        
        try {
            // Write some data first
            for (int i = 0; i < 100; i++) {
                String data = "Test data " + i;
                mappedFile.appendMessage(data.getBytes());
            }
            
            ExecutorService executor = Executors.newFixedThreadPool(5);
            CountDownLatch latch = new CountDownLatch(5);
            AtomicInteger flushCount = new AtomicInteger(0);
            
            for (int i = 0; i < 5; i++) {
                executor.submit(() -> {
                    try {
                        int flushed = mappedFile.flush(0);
                        if (flushed > 0) {
                            flushCount.incrementAndGet();
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    } finally {
                        latch.countDown();
                    }
                });
            }
            
            latch.await();
            executor.shutdown();
            
            Assert.assertTrue("At least one flush should succeed", flushCount.get() > 0);
            
        } finally {
            mappedFile.destroy(0);
        }
    }
}
