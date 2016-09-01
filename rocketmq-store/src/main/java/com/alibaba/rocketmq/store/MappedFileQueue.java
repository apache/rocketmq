/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package com.alibaba.rocketmq.store;

import com.alibaba.rocketmq.common.UtilAll;
import com.alibaba.rocketmq.common.constant.LoggerName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;


/**
 * @author shijia.wxr
 */
public class MappedFileQueue {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.StoreLoggerName);
    private static final Logger logError = LoggerFactory.getLogger(LoggerName.StoreErrorLoggerName);

    private static final int DeleteFilesBatchMax = 10;

    private final String storePath;

    private final int mappedFileSize;

    private final CopyOnWriteArrayList<MappedFile> mappedFiles = new CopyOnWriteArrayList<MappedFile>();
    private final ReadWriteLock readWriteLock = new ReentrantReadWriteLock();

    private final AllocateMappedFileService allocateMappedFileService;

    private long flushedWhere = 0;
    private long committedWhere = 0;

    private volatile long storeTimestamp = 0;


    public MappedFileQueue(final String storePath, int mappedFileSize,
                           AllocateMappedFileService allocateMappedFileService) {
        this.storePath = storePath;
        this.mappedFileSize = mappedFileSize;
        this.allocateMappedFileService = allocateMappedFileService;
    }


    public void checkSelf() {
        this.readWriteLock.readLock().lock();
        try {
            if (!this.mappedFiles.isEmpty()) {
                MappedFile first = this.mappedFiles.get(0);
                MappedFile last = this.mappedFiles.get(this.mappedFiles.size() - 1);

                int sizeCompute =
                        (int) ((last.getFileFromOffset() - first.getFileFromOffset()) / this.mappedFileSize) + 1;
                int sizeReal = this.mappedFiles.size();
                if (sizeCompute != sizeReal) {
                    logError
                            .error(
                                    "[BUG]The mapedfile queue's data is damaged, {} mappedFileSize={} sizeCompute={} sizeReal={}\n{}", //
                                    this.storePath,//
                                    this.mappedFileSize,//
                                    sizeCompute,//
                                    sizeReal,//
                                    this.mappedFiles.toString()//
                            );
                }
            }
        } finally {
            this.readWriteLock.readLock().unlock();
        }
    }


    public MappedFile getMapedFileByTime(final long timestamp) {
        Object[] mfs = this.copyMapedFiles(0);

        if (null == mfs)
            return null;

        for (int i = 0; i < mfs.length; i++) {
            MappedFile mappedFile = (MappedFile) mfs[i];
            if (mappedFile.getLastModifiedTimestamp() >= timestamp) {
                return mappedFile;
            }
        }

        return (MappedFile) mfs[mfs.length - 1];
    }


    private Object[] copyMapedFiles(final int reservedMapedFiles) {
        Object[] mfs = null;

        try {
            this.readWriteLock.readLock().lock();
            if (this.mappedFiles.size() <= reservedMapedFiles) {
                return null;
            }

            mfs = this.mappedFiles.toArray();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            this.readWriteLock.readLock().unlock();
        }
        return mfs;
    }


    public void truncateDirtyFiles(long offset) {
        List<MappedFile> willRemoveFiles = new ArrayList<MappedFile>();

        for (MappedFile file : this.mappedFiles) {
            long fileTailOffset = file.getFileFromOffset() + this.mappedFileSize;
            if (fileTailOffset > offset) {
                if (offset >= file.getFileFromOffset()) {
                    file.setWrotePostion((int) (offset % this.mappedFileSize));
                    file.setFlushedPosition((int) (offset % this.mappedFileSize));
                } else {

                    file.destroy(1000);
                    willRemoveFiles.add(file);
                }
            }
        }

        this.deleteExpiredFile(willRemoveFiles);
    }


    private void deleteExpiredFile(List<MappedFile> files) {
        if (!files.isEmpty()) {
            try {
                this.readWriteLock.writeLock().lock();
                for (MappedFile file : files) {
                    if (!this.mappedFiles.remove(file)) {
                        log.error("deleteExpiredFile remove failed.");
                        break;
                    }
                }
            } catch (Exception e) {
                log.error("deleteExpiredFile has exception.", e);
            } finally {
                this.readWriteLock.writeLock().unlock();
            }
        }
    }


    public boolean load() {
        File dir = new File(this.storePath);
        File[] files = dir.listFiles();
        if (files != null) {
            // ascending order
            Arrays.sort(files);
            for (File file : files) {

                if (file.length() != this.mappedFileSize) {
                    log.warn(file + "\t" + file.length()
                            + " length not matched message store config value, ignore it");
                    return true;
                }


                try {
                    MappedFile mappedFile = new MappedFile(file.getPath(), mappedFileSize);

                    mappedFile.setWrotePostion(this.mappedFileSize);
                    mappedFile.setFlushedPosition(this.mappedFileSize);
                    this.mappedFiles.add(mappedFile);
                    log.info("load " + file.getPath() + " OK");
                } catch (IOException e) {
                    log.error("load file " + file + " error", e);
                    return false;
                }
            }
        }

        return true;
    }


    public long howMuchFallBehind() {
        if (this.mappedFiles.isEmpty())
            return 0;

        long committed = this.flushedWhere;
        if (committed != 0) {
            MappedFile mappedFile = this.getLastMapedFile(0, false);
            if (mappedFile != null) {
                return (mappedFile.getFileFromOffset() + mappedFile.getWrotePostion()) - committed;
            }
        }

        return 0;
    }


    public MappedFile getLastMapedFile(final long startOffset, boolean needCreate) {
        long createOffset = -1;
        MappedFile mappedFileLast = null;
        {
            this.readWriteLock.readLock().lock();
            if (this.mappedFiles.isEmpty()) {
                createOffset = startOffset - (startOffset % this.mappedFileSize);
            } else {
                mappedFileLast = this.mappedFiles.get(this.mappedFiles.size() - 1);
            }
            this.readWriteLock.readLock().unlock();
        }

        if (mappedFileLast != null && mappedFileLast.isFull()) {
            createOffset = mappedFileLast.getFileFromOffset() + this.mappedFileSize;
        }

        if (createOffset != -1 && needCreate) {
            String nextFilePath = this.storePath + File.separator + UtilAll.offset2FileName(createOffset);
            String nextNextFilePath =
                    this.storePath + File.separator
                            + UtilAll.offset2FileName(createOffset + this.mappedFileSize);
            MappedFile mappedFile = null;

            if (this.allocateMappedFileService != null) {
                mappedFile =
                        this.allocateMappedFileService.putRequestAndReturnMapedFile(nextFilePath,
                                nextNextFilePath, this.mappedFileSize);
            } else {
                try {
                    mappedFile = new MappedFile(nextFilePath, this.mappedFileSize);
                } catch (IOException e) {
                    log.error("create mapedfile exception", e);
                }
            }

            if (mappedFile != null) {
                this.readWriteLock.writeLock().lock();
                if (this.mappedFiles.isEmpty()) {
                    mappedFile.setFirstCreateInQueue(true);
                }
                this.mappedFiles.add(mappedFile);
                this.readWriteLock.writeLock().unlock();
            }

            return mappedFile;
        }

        return mappedFileLast;
    }

    public MappedFile getLastMapedFile() {
        return this.getLastMapedFile(0);
    }

    public MappedFile getLastMapedFile(final long startOffset) {
        return getLastMapedFile(startOffset, true);
    }

    public MappedFile getLastMapedFileWithLock() {
        MappedFile mappedFileLast = null;
        this.readWriteLock.readLock().lock();
        if (!this.mappedFiles.isEmpty()) {
            mappedFileLast = this.mappedFiles.get(this.mappedFiles.size() - 1);
        }
        this.readWriteLock.readLock().unlock();

        return mappedFileLast;
    }

    public boolean resetOffset(long offset) {
        this.readWriteLock.writeLock().lock();
        if (!this.mappedFiles.isEmpty()) {
            MappedFile mappedFileLast = this.mappedFiles.get(this.mappedFiles.size() - 1);
            long lastOffset = mappedFileLast.getFileFromOffset() +
                    mappedFileLast.getWrotePostion();
            long diff = lastOffset - offset;

            final int maxdiff = 1024 * 1024 * 1024 * 2;

            if (diff > maxdiff) return false;
        }

        for (int i = this.mappedFiles.size() - 1; i >= 0; i--) {
            MappedFile mappedFileLast = this.mappedFiles.get(i);

            if (offset >= mappedFileLast.getFileFromOffset()) {
                int where = (int) (offset % mappedFileLast.getFileSize());
                mappedFileLast.setFlushedPosition(where);
                mappedFileLast.setWrotePostion(where);
                break;
            } else {
                this.mappedFiles.remove(mappedFileLast);
            }
        }
        this.readWriteLock.writeLock().unlock();
        return true;
    }

    public long getMinOffset() {
        try {
            this.readWriteLock.readLock().lock();
            if (!this.mappedFiles.isEmpty()) {
                return this.mappedFiles.get(0).getFileFromOffset();
            }
        } catch (Exception e) {
            log.error("getMinOffset has exception.", e);
        } finally {
            this.readWriteLock.readLock().unlock();
        }

        return -1;
    }


    public long getMaxOffset() {
        try {
            this.readWriteLock.readLock().lock();
            if (!this.mappedFiles.isEmpty()) {
                int lastIndex = this.mappedFiles.size() - 1;
                MappedFile mappedFile = this.mappedFiles.get(lastIndex);
                return mappedFile.getFileFromOffset() + mappedFile.getWrotePostion();
            }
        } catch (Exception e) {
            log.error("getMinOffset has exception.", e);
        } finally {
            this.readWriteLock.readLock().unlock();
        }

        return 0;
    }

    public void deleteLastMapedFile() {
        if (!this.mappedFiles.isEmpty()) {
            int lastIndex = this.mappedFiles.size() - 1;
            MappedFile mappedFile = this.mappedFiles.get(lastIndex);
            mappedFile.destroy(1000);
            this.mappedFiles.remove(mappedFile);
            log.info("on recover, destroy a logic maped file " + mappedFile.getFileName());
        }
    }

    public int deleteExpiredFileByTime(//
                                       final long expiredTime, //
                                       final int deleteFilesInterval, //
                                       final long intervalForcibly,//
                                       final boolean cleanImmediately//
    ) {
        Object[] mfs = this.copyMapedFiles(0);

        if (null == mfs)
            return 0;


        int mfsLength = mfs.length - 1;
        int deleteCount = 0;
        List<MappedFile> files = new ArrayList<MappedFile>();
        if (null != mfs) {
            for (int i = 0; i < mfsLength; i++) {
                MappedFile mappedFile = (MappedFile) mfs[i];
                long liveMaxTimestamp = mappedFile.getLastModifiedTimestamp() + expiredTime;
                if (System.currentTimeMillis() >= liveMaxTimestamp//
                        || cleanImmediately) {
                    if (mappedFile.destroy(intervalForcibly)) {
                        files.add(mappedFile);
                        deleteCount++;

                        if (files.size() >= DeleteFilesBatchMax) {
                            break;
                        }

                        if (deleteFilesInterval > 0 && (i + 1) < mfsLength) {
                            try {
                                Thread.sleep(deleteFilesInterval);
                            } catch (InterruptedException e) {
                            }
                        }
                    } else {
                        break;
                    }
                }
            }
        }

        deleteExpiredFile(files);

        return deleteCount;
    }


    public int deleteExpiredFileByOffset(long offset, int unitSize) {
        Object[] mfs = this.copyMapedFiles(0);

        List<MappedFile> files = new ArrayList<MappedFile>();
        int deleteCount = 0;
        if (null != mfs) {

            int mfsLength = mfs.length - 1;

            for (int i = 0; i < mfsLength; i++) {
                boolean destroy = true;
                MappedFile mappedFile = (MappedFile) mfs[i];
                SelectMapedBufferResult result = mappedFile.selectMapedBuffer(this.mappedFileSize - unitSize);
                if (result != null) {
                    long maxOffsetInLogicQueue = result.getByteBuffer().getLong();
                    result.release();

                    destroy = (maxOffsetInLogicQueue < offset);
                    if (destroy) {
                        log.info("physic min offset " + offset + ", logics in current mapedfile max offset "
                                + maxOffsetInLogicQueue + ", delete it");
                    }
                } else {
                    log.warn("this being not excuted forever.");
                    break;
                }

                if (destroy && mappedFile.destroy(1000 * 60)) {
                    files.add(mappedFile);
                    deleteCount++;
                } else {
                    break;
                }
            }
        }

        deleteExpiredFile(files);

        return deleteCount;
    }


    public boolean flush(final int flushLeastPages) {
        boolean result = true;
        MappedFile mappedFile = this.findMapedFileByOffset(this.flushedWhere, true);
        if (mappedFile != null) {
            long tmpTimeStamp = mappedFile.getStoreTimestamp();
            int offset = mappedFile.flush(flushLeastPages);
            long where = mappedFile.getFileFromOffset() + offset;
            result = (where == this.flushedWhere);
            this.flushedWhere = where;
            if (0 == flushLeastPages) {
                this.storeTimestamp = tmpTimeStamp;
            }
        }

        return result;
    }

    public boolean commit(final int commitLeastPages) {
        boolean result = true;
        MappedFile mappedFile = this.findMapedFileByOffset(this.committedWhere, true);
        if (mappedFile != null) {
            int offset = mappedFile.commit(commitLeastPages);
            long where = mappedFile.getFileFromOffset() + offset;
            result = (where == this.committedWhere);
            this.committedWhere = where;
        }

        return result;
    }


    public MappedFile findMapedFileByOffset(final long offset, final boolean returnFirstOnNotFound) {
        try {
            this.readWriteLock.readLock().lock();
            MappedFile mappedFile = this.getFirstMapedFile();

            if (mappedFile != null) {
                int index =
                        (int) ((offset / this.mappedFileSize) - (mappedFile.getFileFromOffset() / this.mappedFileSize));
                if (index < 0 || index >= this.mappedFiles.size()) {
                    logError
                            .warn(
                                    "findMapedFileByOffset offset not matched, request Offset: {}, index: {}, mappedFileSize: {}, mappedFiles count: {}, StackTrace: {}",//
                                    offset,//
                                    index,//
                                    this.mappedFileSize,//
                                    this.mappedFiles.size(),//
                                    UtilAll.currentStackTrace());
                }

                try {
                    return this.mappedFiles.get(index);
                } catch (Exception e) {
                    if (returnFirstOnNotFound) {
                        return mappedFile;
                    }
                }
            }
        } catch (Exception e) {
            log.error("findMapedFileByOffset Exception", e);
        } finally {
            this.readWriteLock.readLock().unlock();
        }

        return null;
    }


    private MappedFile getFirstMapedFile() {
        if (this.mappedFiles.isEmpty()) {
            return null;
        }

        return this.mappedFiles.get(0);
    }


    public MappedFile getLastMapedFile2() {
        if (this.mappedFiles.isEmpty()) {
            return null;
        }
        return this.mappedFiles.get(this.mappedFiles.size() - 1);
    }


    public MappedFile findMapedFileByOffset(final long offset) {
        return findMapedFileByOffset(offset, false);
    }


    public long getMapedMemorySize() {
        long size = 0;

        Object[] mfs = this.copyMapedFiles(0);
        if (mfs != null) {
            for (Object mf : mfs) {
                if (((ReferenceResource) mf).isAvailable()) {
                    size += this.mappedFileSize;
                }
            }
        }

        return size;
    }


    public boolean retryDeleteFirstFile(final long intervalForcibly) {
        MappedFile mappedFile = this.getFirstMapedFileOnLock();
        if (mappedFile != null) {
            if (!mappedFile.isAvailable()) {
                log.warn("the mapedfile was destroyed once, but still alive, " + mappedFile.getFileName());
                boolean result = mappedFile.destroy(intervalForcibly);
                if (result) {
                    log.warn("the mapedfile redelete OK, " + mappedFile.getFileName());
                    List<MappedFile> tmps = new ArrayList<MappedFile>();
                    tmps.add(mappedFile);
                    this.deleteExpiredFile(tmps);
                } else {
                    log.warn("the mapedfile redelete Failed, " + mappedFile.getFileName());
                }

                return result;
            }
        }

        return false;
    }


    public MappedFile getFirstMapedFileOnLock() {
        try {
            this.readWriteLock.readLock().lock();
            return this.getFirstMapedFile();
        } finally {
            this.readWriteLock.readLock().unlock();
        }
    }


    public void shutdown(final long intervalForcibly) {
        this.readWriteLock.readLock().lock();
        for (MappedFile mf : this.mappedFiles) {
            mf.shutdown(intervalForcibly);
        }
        this.readWriteLock.readLock().unlock();
    }


    public void destroy() {
        this.readWriteLock.writeLock().lock();
        for (MappedFile mf : this.mappedFiles) {
            mf.destroy(1000 * 3);
        }
        this.mappedFiles.clear();
        this.flushedWhere = 0;

        // delete parent directory
        File file = new File(storePath);
        if (file.isDirectory()) {
            file.delete();
        }
        this.readWriteLock.writeLock().unlock();
    }


    public long getFlushedWhere() {
        return flushedWhere;
    }


    public void setFlushedWhere(long flushedWhere) {
        this.flushedWhere = flushedWhere;
    }


    public long getStoreTimestamp() {
        return storeTimestamp;
    }


    public List<MappedFile> getMappedFiles() {
        return mappedFiles;
    }


    public int getMappedFileSize() {
        return mappedFileSize;
    }

    public long getCommittedWhere() {
        return committedWhere;
    }

    public void setCommittedWhere(final long committedWhere) {
        this.committedWhere = committedWhere;
    }
}
