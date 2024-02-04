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

package org.apache.rocketmq.srvutil;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import static org.assertj.core.api.Assertions.assertThat;

@RunWith(MockitoJUnitRunner.class)
public class FileWatchServiceTest {
    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    @Test
    public void watchSingleFile() throws Exception {
        final File file = tempFolder.newFile();
        final Semaphore waitSemaphore = new Semaphore(0);
        FileWatchService fileWatchService = new FileWatchService(new String[] {file.getAbsolutePath()}, path -> {
            assertThat(file.getAbsolutePath()).isEqualTo(path);
            waitSemaphore.release();
        });
        fileWatchService.start();
        fileWatchService.awaitStarted(1000);
        modifyFile(file);
        boolean result = waitSemaphore.tryAcquire(1, 1000, TimeUnit.MILLISECONDS);
        assertThat(result).isTrue();
        fileWatchService.shutdown();
    }

    @Test
    public void watchSingleFile_FileDeleted() throws Exception {
        File file = tempFolder.newFile();
        final Semaphore waitSemaphore = new Semaphore(0);
        FileWatchService fileWatchService = new FileWatchService(new String[] {file.getAbsolutePath()},
            path -> waitSemaphore.release());
        fileWatchService.start();
        fileWatchService.awaitStarted(1000);
        assertThat(file.delete()).isTrue();
        boolean result = waitSemaphore.tryAcquire(1, 1000, TimeUnit.MILLISECONDS);
        assertThat(result).isFalse();
        assertThat(file.createNewFile()).isTrue();
        modifyFile(file);
        result = waitSemaphore.tryAcquire(1, 2000, TimeUnit.MILLISECONDS);
        assertThat(result).isTrue();
        fileWatchService.shutdown();
    }

    @Test
    public void watchTwoFile_FileDeleted() throws Exception {
        File fileA = tempFolder.newFile();
        File fileB = tempFolder.newFile();
        Files.write(fileA.toPath(), "Hello, World!".getBytes(StandardCharsets.UTF_8));
        Files.write(fileB.toPath(), "Hello, World!".getBytes(StandardCharsets.UTF_8));
        final Semaphore waitSemaphore = new Semaphore(0);
        FileWatchService fileWatchService = new FileWatchService(
            new String[] {fileA.getAbsolutePath(), fileB.getAbsolutePath()},
            path -> waitSemaphore.release());
        fileWatchService.start();
        fileWatchService.awaitStarted(1000);
        assertThat(fileA.delete()).isTrue();
        boolean result = waitSemaphore.tryAcquire(1, 1000, TimeUnit.MILLISECONDS);
        assertThat(result).isFalse();
        modifyFile(fileB);
        result = waitSemaphore.tryAcquire(1, 1000, TimeUnit.MILLISECONDS);
        assertThat(result).isTrue();
        assertThat(fileA.createNewFile()).isTrue();
        modifyFile(fileA);
        result = waitSemaphore.tryAcquire(1, 1000, TimeUnit.MILLISECONDS);
        assertThat(result).isTrue();
        fileWatchService.shutdown();
    }

    @Test
    public void watchTwoFiles_ModifyOne() throws Exception {
        final File fileA = tempFolder.newFile();
        File fileB = tempFolder.newFile();
        final Semaphore waitSemaphore = new Semaphore(0);
        FileWatchService fileWatchService = new FileWatchService(
            new String[] {fileA.getAbsolutePath(), fileB.getAbsolutePath()},
            path -> {
                assertThat(path).isEqualTo(fileA.getAbsolutePath());
                waitSemaphore.release();
            });
        fileWatchService.start();
        fileWatchService.awaitStarted(1000);
        modifyFile(fileA);
        boolean result = waitSemaphore.tryAcquire(1, 2000, TimeUnit.MILLISECONDS);
        assertThat(result).isTrue();
        fileWatchService.shutdown();
    }

    @Test
    public void watchTwoFiles() throws Exception {
        File fileA = tempFolder.newFile();
        File fileB = tempFolder.newFile();
        final Semaphore waitSemaphore = new Semaphore(0);
        FileWatchService fileWatchService = new FileWatchService(
            new String[] {fileA.getAbsolutePath(), fileB.getAbsolutePath()},
            path -> waitSemaphore.release());
        fileWatchService.start();
        fileWatchService.awaitStarted(1000);
        modifyFile(fileA);
        modifyFile(fileB);
        boolean result = waitSemaphore.tryAcquire(2, 1000, TimeUnit.MILLISECONDS);
        assertThat(result).isTrue();
    }

    private static void modifyFile(File file) {
        try {
            PrintWriter out = new PrintWriter(file);
            out.println(System.nanoTime());
            out.flush();
            out.close();
        } catch (IOException ignore) {
        }
    }
}