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
package org.apache.rocketmq.tieredstore.provider;

import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import org.apache.rocketmq.tieredstore.provider.stream.FileSegmentInputStream;

public interface TieredStoreProvider {

    /**
     * Get file path in backend file system
     *
     * @return file real path
     */
    String getPath();

    /**
     * Get the real length of the file.
     * Return 0 if the file does not exist,
     * Return -1 if system get size failed.
     *
     * @return file real size
     */
    long getSize();

    /**
     * Is file exists in backend file system
     *
     * @return <code>true</code> if file with given path exists; <code>false</code> otherwise
     */
    boolean exists();

    /**
     * Create file in backend file system
     */
    void createFile();

    /**
     * Destroy file with given path in backend file system
     */
    void destroyFile();

    /**
     * Get data from backend file system
     *
     * @param position the index from where the file will be read
     * @param length   the data size will be read
     * @return data to be read
     */
    CompletableFuture<ByteBuffer> read0(long position, int length);

    /**
     * Put data to backend file system
     *
     * @param inputStream data stream
     * @param position    backend file position to put, used in append mode
     * @param length      data size in stream
     * @param append      try to append or create a new file
     * @return put result, <code>true</code> if data successfully write; <code>false</code> otherwise
     */
    CompletableFuture<Boolean> commit0(FileSegmentInputStream inputStream, long position, int length, boolean append);
}
