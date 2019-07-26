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

import com.sun.jna.NativeLong;
import com.sun.jna.Pointer;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageExtBatch;
import org.apache.rocketmq.store.config.FlushDiskType;
import org.apache.rocketmq.store.util.LibC;
import sun.nio.ch.DirectBuffer;

public class MappedFile extends ReferenceResource {




    public static final int OS_PAGE_SIZE = 1024 * 4; // 操作系统每页大小，默认 4k
    protected static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    //当 前 JVM 实 例中 MappedFile 虚拟内存 。
    private static final AtomicLong TOTAL_MAPPED_VIRTUAL_MEMORY = new AtomicLong(0);

    //当前 JVM 实例中 MappedFile 对象个数
    private static final AtomicInteger TOTAL_MAPPED_FILES = new AtomicInteger(0);
    // 当前该文件的写指针，从0开始(内存映射文件中的写指针)。当前写到哪一个位置.
    protected final AtomicInteger wrotePosition = new AtomicInteger(0);
    //ADD BY ChenYang
    // 当前文件的提交指针，如果开启 transientStorePoolEnable， 则数据会存储在 TransientStorePool 中，
    // 然后提交到内存映射 ByteBuffer 中， 再 刷写到磁盘。已经提交(已经持久化到磁盘)的位置.
    protected final AtomicInteger committedPosition = new AtomicInteger(0);
    // 刷写到磁盘指针，该指针之前的数据持久化到磁盘中,已经提交(已经持久化到磁盘)的位置
    private final AtomicInteger flushedPosition = new AtomicInteger(0);
    //文件大小
    protected int fileSize;
    //文件通道
    protected FileChannel fileChannel;

    // RocketMQ提供两种数据落盘的方式:
    // 一种是直接将数据写到映射文件字节缓冲区(mappedByteBuffer), 映射文件字节缓冲区(mappedByteBuffer) flush;
    // 另一种是先写到writeBuffer, 再从内存字节缓冲区(write buffer)提交(commit)到文件通道(fileChannel), 然后文件通道(fileChannel)flush.
    /**
     * Message will put to here first, and then reput to FileChannel if writeBuffer is not null.
     */
    // 堆内存 ByteBuffer，如果不为空，数据首先将存储在该 Buffer中，
    // 然后提交到MappedFile对应的内存映射文件Buffer。transientStorePoolEnable 为true 时不为空.
    // 内存字节缓冲区
    protected ByteBuffer writeBuffer = null;
    // 直接内存池， transientStorePoolEnable 为 true 时启用 。
    protected TransientStorePool transientStorePool = null;
    private String fileName;// 文件名称 。
    // 映射的起始偏移量, 拿commitlog文件来举例,
    // 下面有很多个文件夹(假设为1KB, 默认是1G大小),
    // 第一个文件名为00000000000000000000,
    // 第二个文件名为00000000000000001024,
    // 那么第一个文件的fileFromOffset就是0,
    // 第二个文件的fileFromOffset就是1024

    private long fileFromOffset; //该文件的初始偏移量
    private File file;
    private MappedByteBuffer mappedByteBuffer; //物理文件对应的内存映射 Buffer, 文件在内存中的映射. 如前文所述RocketMQ使用内存映射的方式来操作文件, 这种方式要比流的方式快很多.
    private volatile long storeTimestamp = 0;  // 文件最后一次内容写入时间
    private boolean firstCreateInQueue = false;//是否是 MappedFileQueue 队列中第一个文件

    public MappedFile() {
    }

    public MappedFile(final String fileName, final int fileSize) throws IOException {
        init(fileName, fileSize);
    }

    public MappedFile(final String fileName, final int fileSize,
        final TransientStorePool transientStorePool) throws IOException {
        init(fileName, fileSize, transientStorePool);
    }

    public static void ensureDirOK(final String dirName) {
        if (dirName != null) {
            File f = new File(dirName);
            if (!f.exists()) {
                boolean result = f.mkdirs();
                log.info(dirName + " mkdir " + (result ? "OK" : "Failed"));
            }
        }
    }

    public static void clean(final ByteBuffer buffer) {
        if (buffer == null || !buffer.isDirect() || buffer.capacity() == 0)
            return;
        invoke(invoke(viewed(buffer), "cleaner"), "clean");
    }

    private static Object invoke(final Object target, final String methodName, final Class<?>... args) {
        return AccessController.doPrivileged(new PrivilegedAction<Object>() {
            public Object run() {
                try {
                    Method method = method(target, methodName, args);
                    method.setAccessible(true);
                    return method.invoke(target);
                } catch (Exception e) {
                    throw new IllegalStateException(e);
                }
            }
        });
    }

    private static Method method(Object target, String methodName, Class<?>[] args)
        throws NoSuchMethodException {
        try {
            return target.getClass().getMethod(methodName, args);
        } catch (NoSuchMethodException e) {
            return target.getClass().getDeclaredMethod(methodName, args);
        }
    }

    private static ByteBuffer viewed(ByteBuffer buffer) {
        String methodName = "viewedBuffer";

        Method[] methods = buffer.getClass().getMethods();
        for (int i = 0; i < methods.length; i++) {
            if (methods[i].getName().equals("attachment")) {
                methodName = "attachment";
                break;
            }
        }

        ByteBuffer viewedBuffer = (ByteBuffer) invoke(buffer, methodName);
        if (viewedBuffer == null)
            return buffer;
        else
            return viewed(viewedBuffer);
    }

    public static int getTotalMappedFiles() {
        return TOTAL_MAPPED_FILES.get();
    }

    public static long getTotalMappedVirtualMemory() {
        return TOTAL_MAPPED_VIRTUAL_MEMORY.get();
    }

    //
    public void init(final String fileName, final int fileSize, final TransientStorePool transientStorePool) throws IOException {
        init(fileName, fileSize);
        // 如果 transientStorePoolEnable 为 true，则初始化 MappedFile 的 writeBuffer， 从transientStorePool
        this.writeBuffer = transientStorePool.borrowBuffer();
        this.transientStorePool = transientStorePool;
    }

    private void init(final String fileName, final int fileSize) throws IOException {
        this.fileName = fileName;
        this.fileSize = fileSize;
        this.file = new File(fileName);
        // 初始化 fileFromOffset 为文件名，也就是文件名代表该文件的起始偏移量，
        // 通过 RandomAccessFile创建读写文件通道，并将文件内容使用NIO的内存映射Buffer将文件映射到内存中。
        this.fileFromOffset = Long.parseLong(this.file.getName());
        boolean ok = false;

        ensureDirOK(this.file.getParent());

        try {
            this.fileChannel = new RandomAccessFile(this.file, "rw").getChannel();
            this.mappedByteBuffer = this.fileChannel.map(MapMode.READ_WRITE, 0, fileSize);
            TOTAL_MAPPED_VIRTUAL_MEMORY.addAndGet(fileSize);
            TOTAL_MAPPED_FILES.incrementAndGet();
            ok = true;
        } catch (FileNotFoundException e) {
            log.error("create file channel " + this.fileName + " Failed. ", e);
            throw e;
        } catch (IOException e) {
            log.error("map file " + this.fileName + " Failed. ", e);
            throw e;
        } finally {
            if (!ok && this.fileChannel != null) {
                this.fileChannel.close();
            }
        }
    }

    public long getLastModifiedTimestamp() {
        return this.file.lastModified();
    }

    public int getFileSize() {
        return fileSize;
    }

    public FileChannel getFileChannel() {
        return fileChannel;
    }

    public AppendMessageResult appendMessage(final MessageExtBrokerInner msg, final AppendMessageCallback cb) {
        return appendMessagesInner(msg, cb);
    }

    public AppendMessageResult appendMessages(final MessageExtBatch messageExtBatch, final AppendMessageCallback cb) {
        return appendMessagesInner(messageExtBatch, cb);
    }

    // 将消息追加到MappedFile中，首先获取 MappedFile 当前写指针，
    // 如果 currentPos大于或等于文件大小则表明文件已写满，
    // 抛出 AppendMessageStatus.UNKNOWN_ ERROR。
    // 如果 currentPos 小于文件大小，
    // 通过 slice()方法创建一个与 MappedFile 的共享 内 存区，
    // 并设置 position 为当前指针 。
    public AppendMessageResult appendMessagesInner(final MessageExt messageExt, final AppendMessageCallback cb) {
        assert messageExt != null;
        assert cb != null;
        // 先获取 MappedFile 当前写指针
        // 获取当前写的位置
        int currentPos = this.wrotePosition.get();

        // 如果 currentPos大于或等于文件大小则表明文件已写满，
        // currentPos小于文件尺寸才能写入
        if (currentPos < this.fileSize) {
            // 获取需要写入的字节缓冲区, 之所以会有writeBuffer != null的判断与使用的刷盘服务有关.
            // 如果 currentPos 小于文件大小，通过 slice() 方法创建一个与 MappedFile 的共享内存区，并设置 position 为当前指针
            ByteBuffer byteBuffer = writeBuffer != null ? writeBuffer.slice() : this.mappedByteBuffer.slice();
            byteBuffer.position(currentPos); // 设置写入的 position
            AppendMessageResult result = null;
            if (messageExt instanceof MessageExtBrokerInner) {
                result = cb.doAppend(this.getFileFromOffset(), byteBuffer, this.fileSize - currentPos, (MessageExtBrokerInner) messageExt);
            } else if (messageExt instanceof MessageExtBatch) {
                result = cb.doAppend(this.getFileFromOffset(), byteBuffer, this.fileSize - currentPos, (MessageExtBatch) messageExt);
            } else {
                return new AppendMessageResult(AppendMessageStatus.UNKNOWN_ERROR);
            }
            this.wrotePosition.addAndGet(result.getWroteBytes());
            this.storeTimestamp = result.getStoreTimestamp();
            return result;
        }
        log.error("MappedFile.appendMessage return null, wrotePosition: {} fileSize: {}", currentPos, this.fileSize);
        return new AppendMessageResult(AppendMessageStatus.UNKNOWN_ERROR);
    }

    public long getFileFromOffset() {
        return this.fileFromOffset;
    }

    public boolean appendMessage(final byte[] data) {
        int currentPos = this.wrotePosition.get();

        if ((currentPos + data.length) <= this.fileSize) {
            try {
                this.fileChannel.position(currentPos);
                this.fileChannel.write(ByteBuffer.wrap(data));
            } catch (Throwable e) {
                log.error("Error occurred when append message to mappedFile.", e);
            }
            this.wrotePosition.addAndGet(data.length);
            return true;
        }

        return false;
    }

    /**
     * Content of data from offset to offset + length will be wrote to file.
     *
     * @param offset The offset of the subarray to be used.
     * @param length The length of the subarray to be used.
     */
    public boolean appendMessage(final byte[] data, final int offset, final int length) {
        int currentPos = this.wrotePosition.get();

        if ((currentPos + length) <= this.fileSize) {
            try {
                this.fileChannel.position(currentPos);
                this.fileChannel.write(ByteBuffer.wrap(data, offset, length));
            } catch (Throwable e) {
                log.error("Error occurred when append message to mappedFile.", e);
            }
            this.wrotePosition.addAndGet(length);
            return true;
        }

        return false;
    }

    /**
     * @return The current flushed position
     */
    // 刷盘指的是将内存中的 数据刷写到磁盘 ，永久存储在磁盘中，其具体 实 现由 MappedFile 的 flush方法实现;

    // 刷盘，直接调用 mappedByteBuffer 或 fileChannel 的 force 方法将内存中的数据持久化到磁盘，
    // 那么 flushedPosition 应该等于 MappedByteBuffer 中的写指针;
    // 如果 writeBuffer不为空， 则 flushedPosition应等于上一次commit指针;
    // 因为上一次提交的数据就是进入到 fileChannel 中的数据;
    // 如 果 writeBuffer 为空，数据是直接进入到 MappedByteBuffer, wrotePosition代表的是MappedByteBuffer中的指针，
    // 故设置 flushedPosition 为wrotePosition。
    public int flush(final int flushLeastPages) {
        if (this.isAbleToFlush(flushLeastPages)) {
            if (this.hold()) {
                int value = getReadPosition();

                try {
                    //We only append data to fileChannel or mappedByteBuffer, never both.
                    if (writeBuffer != null || this.fileChannel.position() != 0) {
                        this.fileChannel.force(false);
                    } else {
                        //  写磁盘，直接调用mappedByteBuffer 或 fileChannel的force方法将内存中的数据持久化到磁盘
                        this.mappedByteBuffer.force();
                    }
                } catch (Throwable e) {
                    log.error("Error occurred when force data to disk.", e);
                }

                //flushedPosition 应该等于 MappedByteBuffer 中的写指针
                this.flushedPosition.set(value);
                this.release();
            } else {
                log.warn("in flush, hold failed, flush offset = " + this.flushedPosition.get());
                this.flushedPosition.set(getReadPosition());
            }
        }
        return this.getFlushedPosition();
    }


    //内存映射文件的提交动作由 MappedFile 的 commit方法实现,
    //执行提交操作， commitLeastPages 为本次提交最小的页数
    public int commit(final int commitLeastPages) {
        if (writeBuffer == null) {
            //no need to commit data to file channel, so just regard wrotePosition as committedPosition.
            return this.wrotePosition.get();
        }
        // 如果待提交数据不满 commitLeastPages，则不执行本次提交操作，待下次提交
        if (this.isAbleToCommit(commitLeastPages)) {
            if (this.hold()) {
                commit0(commitLeastPages);
                this.release();
            } else {
                log.warn("in commit, hold failed, commit offset = " + this.committedPosition.get());
            }
        }

        //所有的脏数据都被存储到了fileChannel
        // All dirty data has been committed to FileChannel.
        if (writeBuffer != null && this.transientStorePool != null && this.fileSize == this.committedPosition.get()) {
            this.transientStorePool.returnBuffer(writeBuffer);
            this.writeBuffer = null;
        }
        // writeBuffer如果为空，直接返回 wrotePosition 指针，无须执行 commit操作， 表明 commit操作主体是 writeBuffer。
        return this.committedPosition.get();
    }



    // 具体的提交实现。 首先创建 writeBuffer的共享缓存区，然后将新创建的 position 回 退到上一次提交的位置( committedPosition)， 设置 limit为 wrotePosition (当前最大有效 数据指针)，然后把commitedPosition到wrotePosition的数据复制 (写入)到FileChannel
    // 中， 然后更新committedPosition指针为wrotePositiono commit的作用就是将Mapp巳dFile#­ writeBuffer 中的数据提交到文件通道 FileChannel 中。
    protected void commit0(final int commitLeastPages) {
        int writePos = this.wrotePosition.get();
        int lastCommittedPosition = this.committedPosition.get();

        if (writePos - this.committedPosition.get() > 0) {
            try {
                //ByteBuffer 使用技巧 : slice() 方法创建 一 个共享缓存区 ， 与原先的 ByteBuffer 共享内存 但维护一套独立的指针 (position、 mark、 limit)。
                ByteBuffer byteBuffer = writeBuffer.slice();//方法根据现有的缓冲区创建一个 子缓冲区。也就是它创建一个新的缓冲区，新缓冲区与原来的缓冲区的一部分共享数据。
                byteBuffer.position(lastCommittedPosition);
                byteBuffer.limit(writePos);
                this.fileChannel.position(lastCommittedPosition);
                this.fileChannel.write(byteBuffer);
                this.committedPosition.set(writePos);
            } catch (Throwable e) {
                log.error("Error occurred when commit data to FileChannel.", e);
            }
        }
    }

    //
    private boolean isAbleToFlush(final int flushLeastPages) {
        int flush = this.flushedPosition.get();
        int write = getReadPosition();

        if (this.isFull()) {
            return true;
        }

        if (flushLeastPages > 0) {
            return ((write / OS_PAGE_SIZE) - (flush / OS_PAGE_SIZE)) >= flushLeastPages;
        }

        return write > flush;
    }


    // 比较 wrotePosition( 当前 writeBuffer 的写指针)与上 一次提交的指针(committedPosition) 的差值，
    // 除以 OS_PAGE_SIZE得到当前脏页的数量，如果大于 commitLeastPages则返回 true;
    // 如果 commitLeastPages 小 于 0 表示只要存在脏页就提交 。
    protected boolean isAbleToCommit(final int commitLeastPages) {
        int flush = this.committedPosition.get();
        int write = this.wrotePosition.get();

        if (this.isFull()) {
            return true;
        }

        if (commitLeastPages > 0) {
            return ((write / OS_PAGE_SIZE) - (flush / OS_PAGE_SIZE)) >= commitLeastPages;
        }

        return write > flush;
    }

    public int getFlushedPosition() {
        return flushedPosition.get();
    }

    public void setFlushedPosition(int pos) {
        this.flushedPosition.set(pos);
    }

    // 文件已满
    public boolean isFull() {
        return this.fileSize == this.wrotePosition.get();
    }


    // 查找 pos 到当前最大可读之间的数据，由于在整个写入期间都未曾改变 MappedByte­ Buffer 的指针 ，
    // 所以 mappedByteBuffer.slice()方法返回的共享缓存区空间为整个 MappedFile，
    // 然后通过设 置 byteBuffer 的 position 为待查找的值，
    // 读取字节为当前可读 字节长度，
    // 最终返回的 ByteBuffer 的 limit (可读 最大长度)为 size。
    public SelectMappedBufferResult selectMappedBuffer(int pos, int size) {
        int readPosition = getReadPosition();
        if ((pos + size) <= readPosition) {

            if (this.hold()) {
                // 操作 ByteBuffer 时,如果使用了 slice() 方法，对其 ByteBuffer 进行读取时一般手动指定
                // position 与 limit 指针，而不是调用 flip 方法来切换读写状态 。
                ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
                byteBuffer.position(pos);
                ByteBuffer byteBufferNew = byteBuffer.slice();
                byteBufferNew.limit(size);
                return new SelectMappedBufferResult(this.fileFromOffset + pos, byteBufferNew, size, this);
            } else {
                log.warn("matched, but hold failed, request pos: " + pos + ", fileFromOffset: "
                    + this.fileFromOffset);
            }
        } else {
            log.warn("selectMappedBuffer request pos invalid, request pos: " + pos + ", size: " + size
                + ", fileFromOffset: " + this.fileFromOffset);
        }

        return null;
    }

    public SelectMappedBufferResult selectMappedBuffer(int pos) {
        int readPosition = getReadPosition();
        if (pos < readPosition && pos >= 0) {
            if (this.hold()) {
                ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
                byteBuffer.position(pos);
                int size = readPosition - pos;
                ByteBuffer byteBufferNew = byteBuffer.slice();
                byteBufferNew.limit(size);
                return new SelectMappedBufferResult(this.fileFromOffset + pos, byteBufferNew, size, this);
            }
        }

        return null;
    }

    // 如果 available 为true，表示 MappedFile 当前可用，无须清理，返回 false ;
    // 如果资 源已经被清除，返回 true ;
    // 如果是堆外内存，调用堆外内存的cleanup方法清除，
    // 维护 MappedFile类变量 TOTAL_MAPPED_VIRTUAL_MEMORY、 TOTAL_MAPPED_FILES并 返回 true，表示 cleanupOver为 true。
    @Override
    public boolean cleanup(final long currentRef) {
        if (this.isAvailable()) {
            log.error("this file[REF:" + currentRef + "] " + this.fileName
                + " have not shutdown, stop unmapping.");
            return false;
        }

        if (this.isCleanupOver()) {
            log.error("this file[REF:" + currentRef + "] " + this.fileName
                + " have cleanup, do not do it again.");
            return true;
        }

        clean(this.mappedByteBuffer);
        TOTAL_MAPPED_VIRTUAL_MEMORY.addAndGet(this.fileSize * (-1));
        TOTAL_MAPPED_FILES.decrementAndGet();
        log.info("unmap file[REF:" + currentRef + "] " + this.fileName + " OK");
        return true;
    }


    // MappedFile 文件销毁的实现方法为 public boolean destroy(final long intervalForcibly), intervalForcibly 表示拒绝被销毁的最大存活时间
    public boolean destroy(final long intervalForcibly) {

        // Step1 : 关闭 MappedFile。 初次调用时 this.available 为 true，设置 available 为 false，
        // 并设置初次关闭的时间戳(firstShutdownTimestamp)为当前时间戳，
        // 然后调用 release()方法尝试释放资源，
        // release 只有在引用次数小于1的情况下才会释放资源;
        // 如果引用次数大于 0，对比当前时间与 firstShutdownTimestamp，
        // 如果已经超过了其最大拒绝存活期，每执行一次，将引用数减少 1000，直到引用数小于 0 时通过执行 release 方法释放资源 。
        this.shutdown(intervalForcibly);

        // Step2 : 判断是否清理完成，判断标准是引用次数小于等于0并且cleanupOver为true,
        // cleanupOver为true的触发条件是release成功将MappedByteBuffer源释放。 稍后详细分 析 release方法。
        if (this.isCleanupOver()) {
            try {
                // Step3: 关闭文件通道， 删除物理文件。
                this.fileChannel.close();
                log.info("close file channel " + this.fileName + " OK");

                long beginTime = System.currentTimeMillis();
                boolean result = this.file.delete();
                log.info("delete file[REF:" + this.getRefCount() + "] " + this.fileName
                    + (result ? " OK, " : " Failed, ") + "W:" + this.getWrotePosition() + " M:"
                    + this.getFlushedPosition() + ", "
                    + UtilAll.computeEclipseTimeMilliseconds(beginTime));
            } catch (Exception e) {
                log.warn("close file channel " + this.fileName + " Failed. ", e);
            }

            return true;
        } else {
            log.warn("destroy mapped file[REF:" + this.getRefCount() + "] " + this.fileName
                + " Failed. cleanupOver: " + this.cleanupOver);
        }

        return false;
    }

    public int getWrotePosition() {
        return wrotePosition.get();
    }

    public void setWrotePosition(int pos) {
        this.wrotePosition.set(pos);
    }

    /**
     * @return The max position which have valid data
     */
    // 如果writeBuffer不为空， 则 flushedPosition 应等于上一次 commit指针
    // 因为上一次提交的数据就是进入到 MappedByteBuffer 中的数据;
    // 如果 writeBuffer 为空，数据是直接进入到 Mapped­ByteBuffer, wrotePosition 代表的是 MappedByteBuffer 中的指针，故设置 flushedPosition 为 wrotePosition。

    //获取当前文件最大的可读指针 。 如果 writeBuffer 为 空，
    // 则 直接 返回当前的 写指针;如 果 writeBuffer不为空， 则返回上一次提交的指针。 在 MappedFile设计中，只有提交了的数 据(写入到 MappedByteBuffer或 FileChannel 中的数据 )才是安全的数据。
    public int getReadPosition() {
        return this.writeBuffer == null ? this.wrotePosition.get() : this.committedPosition.get();
    }

    public void setCommittedPosition(int pos) {
        this.committedPosition.set(pos);
    }

    public void warmMappedFile(FlushDiskType type, int pages) {
        long beginTime = System.currentTimeMillis();
        ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
        int flush = 0;
        long time = System.currentTimeMillis();
        for (int i = 0, j = 0; i < this.fileSize; i += MappedFile.OS_PAGE_SIZE, j++) {
            byteBuffer.put(i, (byte) 0);
            // force flush when flush disk type is sync
            if (type == FlushDiskType.SYNC_FLUSH) {
                if ((i / OS_PAGE_SIZE) - (flush / OS_PAGE_SIZE) >= pages) {
                    flush = i;
                    mappedByteBuffer.force();
                }
            }

            // prevent gc
            if (j % 1000 == 0) {
                log.info("j={}, costTime={}", j, System.currentTimeMillis() - time);
                time = System.currentTimeMillis();
                try {
                    Thread.sleep(0);
                } catch (InterruptedException e) {
                    log.error("Interrupted", e);
                }
            }
        }

        // force flush when prepare load finished
        if (type == FlushDiskType.SYNC_FLUSH) {
            log.info("mapped file warm-up done, force to disk, mappedFile={}, costTime={}",
                this.getFileName(), System.currentTimeMillis() - beginTime);
            mappedByteBuffer.force();
        }
        log.info("mapped file warm-up done. mappedFile={}, costTime={}", this.getFileName(),
            System.currentTimeMillis() - beginTime);

        this.mlock();
    }

    public String getFileName() {
        return fileName;
    }

    public MappedByteBuffer getMappedByteBuffer() {
        return mappedByteBuffer;
    }

    public ByteBuffer sliceByteBuffer() {
        return this.mappedByteBuffer.slice();
    }

    public long getStoreTimestamp() {
        return storeTimestamp;
    }

    public boolean isFirstCreateInQueue() {
        return firstCreateInQueue;
    }

    public void setFirstCreateInQueue(boolean firstCreateInQueue) {
        this.firstCreateInQueue = firstCreateInQueue;
    }

    public void mlock() {
        final long beginTime = System.currentTimeMillis();
        final long address = ((DirectBuffer) (this.mappedByteBuffer)).address();
        Pointer pointer = new Pointer(address);
        {
            int ret = LibC.INSTANCE.mlock(pointer, new NativeLong(this.fileSize));
            log.info("mlock {} {} {} ret = {} time consuming = {}", address, this.fileName, this.fileSize, ret, System.currentTimeMillis() - beginTime);
        }

        {
            int ret = LibC.INSTANCE.madvise(pointer, new NativeLong(this.fileSize), LibC.MADV_WILLNEED);
            log.info("madvise {} {} {} ret = {} time consuming = {}", address, this.fileName, this.fileSize, ret, System.currentTimeMillis() - beginTime);
        }
    }

    public void munlock() {
        final long beginTime = System.currentTimeMillis();
        final long address = ((DirectBuffer) (this.mappedByteBuffer)).address();
        Pointer pointer = new Pointer(address);
        int ret = LibC.INSTANCE.munlock(pointer, new NativeLong(this.fileSize));
        log.info("munlock {} {} {} ret = {} time consuming = {}", address, this.fileName, this.fileSize, ret, System.currentTimeMillis() - beginTime);
    }

    //testable
    File getFile() {
        return this.file;
    }

    @Override
    public String toString() {
        return this.fileName;
    }
}
