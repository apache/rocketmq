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

package org.apache.rocketmq.store.ha.autoswitch;

import com.sun.jna.WString;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class WindowsDirectorySyncTest {
    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Test
    public void forceDefaultFilesystemAfterNamespaceChanges() throws IOException {
        Path directory = temporaryFolder.newFolder("history-\u6062\u590d").toPath();
        Path receipt = Files.createFile(directory.resolve("received"));
        HistoryIncident.forceDirectory(directory);
        Path state = Files.move(receipt, directory.resolve("incident"), StandardCopyOption.ATOMIC_MOVE);
        HistoryIncident.forceDirectory(directory);
        Files.delete(state);
        HistoryIncident.forceDirectory(directory);
    }

    @Test
    public void flushesWritableDirectoryAndClosesHandleOnFailure() throws IOException {
        WindowsDirectorySync.Kernel32 kernel = mock(WindowsDirectorySync.Kernel32.class);
        WindowsDirectorySync.Handle handle = mock(WindowsDirectorySync.Handle.class);
        WString name = new WString("C:\\store");
        when(kernel.CreateFileW(name, 0x40000000, 7, null, 3, 0x02000000, null)).thenReturn(handle);
        when(kernel.GetLastError()).thenReturn(5);
        when(kernel.CloseHandle(handle)).thenReturn(true);
        assertThatThrownBy(() -> WindowsDirectorySync.force(name, "NTFS", kernel))
            .isInstanceOf(IOException.class).hasMessageContaining("FlushFileBuffers").hasMessageContaining("5");
        verify(kernel).FlushFileBuffers(handle);
        verify(kernel).CloseHandle(handle);
    }

    @Test
    public void rejectsUnsupportedFilesystemAndInvalidDirectoryHandle() {
        WindowsDirectorySync.Kernel32 kernel = mock(WindowsDirectorySync.Kernel32.class);
        WString name = new WString("C:\\store");
        assertThatThrownBy(() -> WindowsDirectorySync.force(name, "FAT32", kernel))
            .isInstanceOf(IOException.class).hasMessageContaining("NTFS");
        verify(kernel, never()).CreateFileW(name, 0x40000000, 7, null, 3, 0x02000000, null);

        WindowsDirectorySync.Handle invalid = mock(WindowsDirectorySync.Handle.class);
        when(invalid.isInvalid()).thenReturn(true);
        when(kernel.CreateFileW(name, 0x40000000, 7, null, 3, 0x02000000, null)).thenReturn(invalid);
        assertThatThrownBy(() -> WindowsDirectorySync.force(name, "NTFS", kernel))
            .isInstanceOf(IOException.class).hasMessageContaining("CreateFileW");
        verify(kernel, never()).FlushFileBuffers(invalid);
        verify(kernel, never()).CloseHandle(invalid);
    }
}
