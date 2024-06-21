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
package org.apache.rocketmq.store;

import org.apache.rocketmq.store.logfile.MappedFile;

public class FileQueueSnapshot {
    private MappedFile firstFile;
    private long firstFileIndex;
    private MappedFile lastFile;
    private long lastFileIndex;
    private long currentFile;
    private long currentFileIndex;
    private long behindCount;
    private boolean exist;

    public FileQueueSnapshot() {
    }

    public FileQueueSnapshot(MappedFile firstFile, long firstFileIndex, MappedFile lastFile, long lastFileIndex, long currentFile, long currentFileIndex, long behindCount, boolean exist) {
        this.firstFile = firstFile;
        this.firstFileIndex = firstFileIndex;
        this.lastFile = lastFile;
        this.lastFileIndex = lastFileIndex;
        this.currentFile = currentFile;
        this.currentFileIndex = currentFileIndex;
        this.behindCount = behindCount;
        this.exist = exist;
    }

    public MappedFile getFirstFile() {
        return firstFile;
    }

    public long getFirstFileIndex() {
        return firstFileIndex;
    }

    public MappedFile getLastFile() {
        return lastFile;
    }

    public long getLastFileIndex() {
        return lastFileIndex;
    }

    public long getCurrentFile() {
        return currentFile;
    }

    public long getCurrentFileIndex() {
        return currentFileIndex;
    }

    public long getBehindCount() {
        return behindCount;
    }

    public boolean isExist() {
        return exist;
    }

    @Override
    public String toString() {
        return "FileQueueSnapshot{" +
                "firstFile=" + firstFile +
                ", firstFileIndex=" + firstFileIndex +
                ", lastFile=" + lastFile +
                ", lastFileIndex=" + lastFileIndex +
                ", currentFile=" + currentFile +
                ", currentFileIndex=" + currentFileIndex +
                ", behindCount=" + behindCount +
                ", exist=" + exist +
                '}';
    }
}
