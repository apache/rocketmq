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
        FileWatchService fileWatchService = new FileWatchService(new String[] {file.getAbsolutePath()}, new FileWatchService.Listener() {
            @Override
            public void onChanged(String path) {
                assertThat(file.getAbsolutePath()).isEqualTo(path);
                waitSemaphore.release();
            }
        });
        fileWatchService.start();
        modifyFile(file);
        boolean result = waitSemaphore.tryAcquire(1, 1000, TimeUnit.MILLISECONDS);
        assertThat(result).isTrue();
    }

    @Test
    public void watchSingleFile_FileDeleted() throws Exception {
        File file = tempFolder.newFile();
        final Semaphore waitSemaphore = new Semaphore(0);
        FileWatchService fileWatchService = new FileWatchService(new String[] {file.getAbsolutePath()},
            new FileWatchService.Listener() {
            @Override
            public void onChanged(String path) {
                waitSemaphore.release();
            }
        });
        fileWatchService.start();
        file.delete();
        boolean result = waitSemaphore.tryAcquire(1, 1000, TimeUnit.MILLISECONDS);
        assertThat(result).isFalse();
        file.createNewFile();
        modifyFile(file);
        result = waitSemaphore.tryAcquire(1, 2000, TimeUnit.MILLISECONDS);
        assertThat(result).isTrue();
    }

    @Test
    public void watchTwoFile_FileDeleted() throws Exception {
        File fileA = tempFolder.newFile();
        File fileB = tempFolder.newFile();
        final Semaphore waitSemaphore = new Semaphore(0);
        FileWatchService fileWatchService = new FileWatchService(
            new String[] {fileA.getAbsolutePath(), fileB.getAbsolutePath()},
            new FileWatchService.Listener() {
                @Override
                public void onChanged(String path) {
                    waitSemaphore.release();
                }
            });
        fileWatchService.start();
        fileA.delete();
        boolean result = waitSemaphore.tryAcquire(1, 1000, TimeUnit.MILLISECONDS);
        assertThat(result).isFalse();
        modifyFile(fileB);
        result = waitSemaphore.tryAcquire(1, 1000, TimeUnit.MILLISECONDS);
        assertThat(result).isTrue();
        fileA.createNewFile();
        modifyFile(fileA);
        result = waitSemaphore.tryAcquire(1, 1000, TimeUnit.MILLISECONDS);
        assertThat(result).isTrue();
    }

    @Test
    public void watchTwoFiles_ModifyOne() throws Exception {
        final File fileA = tempFolder.newFile();
        File fileB = tempFolder.newFile();
        final Semaphore waitSemaphore = new Semaphore(0);
        FileWatchService fileWatchService = new FileWatchService(
            new String[] {fileA.getAbsolutePath(), fileB.getAbsolutePath()},
            new FileWatchService.Listener() {
            @Override
            public void onChanged(String path) {
                assertThat(path).isEqualTo(fileA.getAbsolutePath());
                waitSemaphore.release();
            }
        });
        fileWatchService.start();
        modifyFile(fileA);
        boolean result = waitSemaphore.tryAcquire(1, 1000, TimeUnit.MILLISECONDS);
        assertThat(result).isTrue();
    }

    @Test
    public void watchTwoFiles() throws Exception {
        File fileA = tempFolder.newFile();
        File fileB = tempFolder.newFile();
        final Semaphore waitSemaphore = new Semaphore(0);
        FileWatchService fileWatchService = new FileWatchService(
            new String[] {fileA.getAbsolutePath(), fileB.getAbsolutePath()},
            new FileWatchService.Listener() {
                @Override
                public void onChanged(String path) {
                    waitSemaphore.release();
                }
            });
        fileWatchService.start();
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