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
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Iterator;
import org.apache.rocketmq.common.message.MessageExtBatch;
import org.apache.rocketmq.common.message.MessageExtBrokerInner;
import org.apache.rocketmq.store.AppendMessageCallback;
import org.apache.rocketmq.store.AppendMessageResult;
import org.apache.rocketmq.store.CompactionAppendMsgCallback;
import org.apache.rocketmq.store.PutMessageContext;
import org.apache.rocketmq.store.SelectMappedBufferResult;
import org.apache.rocketmq.store.TransientStorePool;
import org.apache.rocketmq.store.config.FlushDiskType;

public interface MappedFile {
    /**
     * Returns the file name of the {@code MappedFile}.
     *
     * @return the file name
     */
    String getFileName();

    /**
     * Change the file name of the {@code MappedFile}.
     *
     * @param fileName the new file name
     */
    boolean renameTo(String fileName);

    /**
     * Returns the file size of the {@code MappedFile}.
     *
     * @return the file size
     */
    int getFileSize();

    /**
     * Returns the {@code FileChannel} behind the {@code MappedFile}.
     *
     * @return the file channel
     */
    FileChannel getFileChannel();

    /**
     * Returns true if this {@code MappedFile} is full and no new messages can be added.
     *
     * @return true if the file is full
     */
    boolean isFull();

    /**
     * Returns true if this {@code MappedFile} is available.
     * <p>
     * The mapped file will be not available if it's shutdown or destroyed.
     *
     * @return true if the file is available
     */
    boolean isAvailable();

    /**
     * Appends a message object to the current {@code MappedFile} with a specific call back.
     *
     * @param message a message to append
     * @param messageCallback the specific call back to execute the real append action
     * @param putMessageContext
     * @return the append result
     */
    AppendMessageResult appendMessage(MessageExtBrokerInner message, AppendMessageCallback messageCallback, PutMessageContext putMessageContext);

    /**
     * Appends a batch message object to the current {@code MappedFile} with a specific call back.
     *
     * @param message a message to append
     * @param messageCallback the specific call back to execute the real append action
     * @param putMessageContext
     * @return the append result
     */
    AppendMessageResult appendMessages(MessageExtBatch message, AppendMessageCallback messageCallback, PutMessageContext putMessageContext);

    AppendMessageResult appendMessage(final ByteBuffer byteBufferMsg, final CompactionAppendMsgCallback cb);

    /**
     * Appends a raw message data represents by a byte array to the current {@code MappedFile}.
     *
     * @param data the byte array to append
     * @return true if success; false otherwise.
     */
    boolean appendMessage(byte[] data);

    /**
     * Appends a raw message data represents by a byte array to the current {@code MappedFile}.
     *
     * @param data the byte buffer to append
     * @return true if success; false otherwise.
     */
    boolean appendMessage(ByteBuffer data);

    /**
     * Appends a raw message data represents by a byte array to the current {@code MappedFile},
     * starting at the given offset in the array.
     *
     * @param data the byte array to append
     * @param offset the offset within the array of the first byte to be read
     * @param length the number of bytes to be read from the given array
     * @return true if success; false otherwise.
     */
    boolean appendMessage(byte[] data, int offset, int length);

    /**
     * Returns the global offset of the current {code MappedFile}, it's a long value of the file name.
     *
     * @return the offset of this file
     */
    long getFileFromOffset();

    /**
     * Flushes the data in cache to disk immediately.
     *
     * @param flushLeastPages the least pages to flush
     * @return the flushed position after the method call
     */
    int flush(int flushLeastPages);

    /**
     * Flushes the data in the secondary cache to page cache or disk immediately.
     *
     * @param commitLeastPages the least pages to commit
     * @return the committed position after the method call
     */
    int commit(int commitLeastPages);

    /**
     * Selects a slice of the mapped byte buffer's sub-region behind the mapped file,
     * starting at the given position.
     *
     * @param pos the given position
     * @param size the size of the returned sub-region
     * @return a {@code SelectMappedBufferResult} instance contains the selected slice
     */
    SelectMappedBufferResult selectMappedBuffer(int pos, int size);

    /**
     * Selects a slice of the mapped byte buffer's sub-region behind the mapped file,
     * starting at the given position.
     *
     * @param pos the given position
     * @return a {@code SelectMappedBufferResult} instance contains the selected slice
     */
    SelectMappedBufferResult selectMappedBuffer(int pos);

    /**
     * Returns the mapped byte buffer behind the mapped file.
     *
     * @return the mapped byte buffer
     */
    MappedByteBuffer getMappedByteBuffer();

    /**
     * Returns a slice of the mapped byte buffer behind the mapped file.
     *
     * @return the slice of the mapped byte buffer
     */
    ByteBuffer sliceByteBuffer();

    /**
     * Returns the store timestamp of the last message.
     *
     * @return the store timestamp
     */
    long getStoreTimestamp();

    /**
     * Returns the last modified timestamp of the file.
     *
     * @return the last modified timestamp
     */
    long getLastModifiedTimestamp();

    /**
     * Get data from a certain pos offset with size byte
     *
     * @param pos a certain pos offset to get data
     * @param size the size of data
     * @param byteBuffer the data
     * @return true if with data; false if no data;
     */
    boolean getData(int pos, int size, ByteBuffer byteBuffer);

    /**
     * Destroys the file and delete it from the file system.
     *
     * @param intervalForcibly If {@code true} then this method will destroy the file forcibly and ignore the reference
     * @return true if success; false otherwise.
     */
    boolean destroy(long intervalForcibly);

    /**
     * Shutdowns the file and mark it unavailable.
     *
     * @param intervalForcibly If {@code true} then this method will shutdown the file forcibly and ignore the reference
     */
    void shutdown(long intervalForcibly);

    /**
     * Decreases the reference count by {@code 1} and clean up the mapped file if the reference count reaches at
     * {@code 0}.
     */
    void release();

    /**
     * Increases the reference count by {@code 1}.
     *
     * @return true if success; false otherwise.
     */
    boolean hold();

    /**
     * Returns true if the current file is first mapped file of some consume queue.
     *
     * @return true or false
     */
    boolean isFirstCreateInQueue();

    /**
     * Sets the flag whether the current file is first mapped file of some consume queue.
     *
     * @param firstCreateInQueue true or false
     */
    void setFirstCreateInQueue(boolean firstCreateInQueue);

    /**
     * Returns the flushed position of this mapped file.
     *
     * @return the flushed posotion
     */
    int getFlushedPosition();

    /**
     * Sets the flushed position of this mapped file.
     *
     * @param flushedPosition the specific flushed position
     */
    void setFlushedPosition(int flushedPosition);

    /**
     * Returns the wrote position of this mapped file.
     *
     * @return the wrote position
     */
    int getWrotePosition();

    /**
     * Sets the wrote position of this mapped file.
     *
     * @param wrotePosition the specific wrote position
     */
    void setWrotePosition(int wrotePosition);

    /**
     * Returns the current max readable position of this mapped file.
     *
     * @return the max readable position
     */
    int getReadPosition();

    /**
     * Sets the committed position of this mapped file.
     *
     * @param committedPosition the specific committed position
     */
    void setCommittedPosition(int committedPosition);

    /**
     * Lock the mapped bytebuffer
     */
    void mlock();

    /**
     * Unlock the mapped bytebuffer
     */
    void munlock();

    /**
     * Warm up the mapped bytebuffer
     * @param type
     * @param pages
     */
    void warmMappedFile(FlushDiskType type, int pages);

    /**
     * Swap map
     */
    boolean swapMap();

    /**
     * Clean pageTable
     */
    void cleanSwapedMap(boolean force);

    /**
     * Get recent swap map time
     */
    long getRecentSwapMapTime();

    /**
     * Get recent MappedByteBuffer access count since last swap
     */
    long getMappedByteBufferAccessCountSinceLastSwap();

    /**
     * Get the underlying file
     * @return
     */
    File getFile();

    /**
     * rename file to add ".delete" suffix
     */
    void renameToDelete();

    /**
     * move the file to the parent directory
     * @throws IOException
     */
    void moveToParent() throws IOException;

    /**
     * Get the last flush time
     * @return
     */
    long getLastFlushTime();

    /**
     * Init mapped file
     * @param fileName file name
     * @param fileSize file size
     * @param transientStorePool transient store pool
     * @throws IOException
     */
    void init(String fileName, int fileSize, TransientStorePool transientStorePool) throws IOException;

    Iterator<SelectMappedBufferResult> iterator(int pos);

    /**
     * Check mapped file is loaded to memory with given position and size
     * @param position start offset of data
     * @param size data size
     * @return data is resided in memory or not
     */
    boolean isLoaded(long position, int size);

    /**
     * Check inactive mapped file and try refresh to reduce the memory usage
     * @param force do we need force refresh or we wait inactive time
     * @param inActiveCheckMs the inactive time for unused
     * @return the mmap file is reloaded or not
     */
    boolean checkInactiveAndRefresh(boolean force, int inActiveCheckMs);
}
