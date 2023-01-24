package org.apache.rocketmq.tieredstore.provider.posix;

import com.google.common.io.ByteStreams;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.CompletableFuture;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.tieredstore.common.TieredMessageStoreConfig;
import org.apache.rocketmq.tieredstore.common.TieredStoreExecutor;
import org.apache.rocketmq.tieredstore.provider.TieredFileSegment;
import org.apache.rocketmq.tieredstore.util.TieredStoreUtil;

public class PosixFileSegment extends TieredFileSegment {
    private static final Logger logger = LoggerFactory.getLogger(TieredStoreUtil.TIERED_STORE_LOGGER_NAME);

    private final String basePath;
    private final String filepath;

    private volatile File file;
    private volatile FileChannel readFileChannel;
    private volatile FileChannel writeFileChannel;

    public PosixFileSegment(FileSegmentType fileType, MessageQueue messageQueue,
        long baseOffset, TieredMessageStoreConfig storeConfig) {
        super(fileType, messageQueue, baseOffset, storeConfig);

        String basePath = storeConfig.getTieredStoreFilepath();
        if (StringUtils.isBlank(basePath) || basePath.endsWith(File.separator)) {
            this.basePath = basePath;
        } else {
            this.basePath = basePath + File.separator;
        }
        this.filepath = this.basePath
            + TieredStoreUtil.getHash(storeConfig.getBrokerClusterName()) + "_" + storeConfig.getBrokerClusterName() + File.separator
            + messageQueue.getBrokerName() + File.separator
            + messageQueue.getTopic() + File.separator
            + messageQueue.getQueueId() + File.separator
            + fileType + File.separator
            + TieredStoreUtil.offset2FileName(baseOffset);
        createFile();
    }

    @Override
    public String getPath() {
        return filepath;
    }

    @Override
    public long getSize() {
        if (exists()) {
            return file.length();
        }
        return -1;
    }

    @Override
    public boolean exists() {
        return file != null && file.exists();
    }

    @Override
    public void createFile() {
        if (file == null) {
            synchronized (this) {
                if (file == null) {
                    File file = new File(filepath);
                    try {
                        File dir = file.getParentFile();
                        if (!dir.exists()) {
                            dir.mkdirs();
                        }

                        file.createNewFile();
                        this.readFileChannel = new RandomAccessFile(file, "r").getChannel();
                        this.writeFileChannel = new RandomAccessFile(file, "rwd").getChannel();
                        this.file = file;
                    } catch (Exception e) {
                        logger.error("PosixFileSegment#createFile: create file {} failed: ", filepath, e);
                    }
                }
            }
        }
    }

    @Override
    public void destroyFile() {
        if (file.exists()) {
            file.delete();
        }

        try {
            if (readFileChannel != null && readFileChannel.isOpen()) {
                readFileChannel.close();
            }
            if (writeFileChannel != null && writeFileChannel.isOpen()) {
                writeFileChannel.close();
            }
        } catch (IOException e) {
            logger.error("PosixFileSegment#destroyFile: destroy file {} failed: ", filepath, e);
        }
    }

    @Override
    public CompletableFuture<ByteBuffer> read0(long position, int length) {
        CompletableFuture<ByteBuffer> future = new CompletableFuture<>();
        ByteBuffer byteBuffer = ByteBuffer.allocate(length);
        try {
            readFileChannel.position(position);
            readFileChannel.read(byteBuffer);
            byteBuffer.flip();
            future.complete(byteBuffer);
        } catch (IOException e) {
            logger.error("PosixFileSegment#read0: read file {} failed: position: {}, length: {}",
                filepath, position, length, e);
            future.completeExceptionally(e);
        }
        return future;
    }

    @Override
    public CompletableFuture<Boolean> commit0(TieredFileSegmentInputStream inputStream, long position, int length,
        boolean append) {
        CompletableFuture<Boolean> future = new CompletableFuture<>();
        try {
            TieredStoreExecutor.COMMIT_EXECUTOR.execute(() -> {
                try {
                    byte[] byteArray = ByteStreams.toByteArray(inputStream);
                    if (byteArray.length != length) {
                        logger.error("PosixFileSegment#commit0: append file {} failed: real data size: {}, is not equal to length: {}",
                            filepath, byteArray.length, length);
                        future.complete(false);
                        return;
                    }
                    writeFileChannel.position(position);
                    ByteBuffer buffer = ByteBuffer.wrap(byteArray);
                    while(buffer.hasRemaining()) {
                        writeFileChannel.write(buffer);
                    }
                    future.complete(true);
                } catch (Exception e) {
                    logger.error("PosixFileSegment#commit0: append file {} failed: position: {}, length: {}",
                        filepath, position, length, e);
                    future.completeExceptionally(e);
                }
            });
        } catch (Exception e) {
            // commit task cannot be executed
            future.completeExceptionally(e);
        }
        return future;
    }
}
