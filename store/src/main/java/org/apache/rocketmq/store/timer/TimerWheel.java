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
package org.apache.rocketmq.store.timer;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

public class TimerWheel {

    private static final Logger log = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);
    private static final int BACKUP_SEGMENT_BYTES = 1024 * 1024;
    public static final String TIMER_WHEEL_FILE_NAME = "timerwheel";
    public static final int BLANK = -1, IGNORE = -2;
    public final int slotsTotal;
    public final int precisionMs;
    private final String fileName;
    private final MappedByteBuffer mappedByteBuffer;
    private final RandomAccessFile randomAccessFile;
    private final FileChannel fileChannel;
    private final ByteBuffer byteBuffer;
    private final ThreadLocal<ByteBuffer> localBuffer = new ThreadLocal<ByteBuffer>() {
        @Override
        protected ByteBuffer initialValue() {
            return byteBuffer.duplicate();
        }
    };
    private final int wheelLength;

    private long snapOffset;
    private ByteBuffer snapshotBuffer;
    // Slots written since the last snapshot freeze. Only maintained in snapshot mode, where every
    // wheel write is serialized by TimerMessageStore#lockWhenFlush with copyForSnapshot().
    private final BitSet dirtySlots;
    private boolean snapshotInitialized;
    private int lastSnapshotCopiedSlots;

    public TimerWheel(String fileName, int slotsTotal, int precisionMs) throws IOException {
        this(fileName, slotsTotal, precisionMs, -1, false);
    }
    public TimerWheel(String fileName, int slotsTotal, int precisionMs, long snapOffset) throws IOException {
        this(fileName, slotsTotal, precisionMs, snapOffset, snapOffset >= 0);
    }
    public TimerWheel(String fileName, int slotsTotal, int precisionMs, long snapOffset, boolean snapshotMode) throws IOException {
        this.slotsTotal = slotsTotal;
        this.precisionMs = precisionMs;
        this.fileName = fileName;
        this.wheelLength = this.slotsTotal * 2 * Slot.SIZE;
        this.snapOffset = snapOffset;
        this.dirtySlots = snapshotMode ? new BitSet(this.slotsTotal * 2) : null;

        String finalFileName = selectSnapshotByFlag(snapOffset);
        File file = new File(finalFileName);
        UtilAll.ensureDirOK(file.getParent());

        try {
            randomAccessFile = new RandomAccessFile(finalFileName, "rw");
            if (file.exists() && randomAccessFile.length() != 0 &&
                randomAccessFile.length() != wheelLength) {
                throw new RuntimeException(String.format("Timer wheel length:%d != expected:%s",
                    randomAccessFile.length(), wheelLength));
            }
            randomAccessFile.setLength(wheelLength);
            // snapshot mode: promoteToBase swaps the base inode, so it must never be mmap'd here.
            if (!snapshotMode) {
                fileChannel = randomAccessFile.getChannel();
                mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, wheelLength);
                assert wheelLength == mappedByteBuffer.remaining();
            } else {
                fileChannel = null;
                mappedByteBuffer = null;
                randomAccessFile.close();
            }
            this.byteBuffer = ByteBuffer.allocateDirect(wheelLength);
            this.byteBuffer.put(Files.readAllBytes(file.toPath()));
        } catch (FileNotFoundException e) {
            log.error("create file channel " + finalFileName + " Failed. ", e);
            throw e;
        } catch (IOException e) {
            log.error("map file " + finalFileName + " Failed. ", e);
            throw e;
        }
    }

    public void shutdown() {
        shutdown(true);
    }

    public void shutdown(boolean flush) {
        if (flush) {
            try {
                this.flush();
            } catch (Throwable e) {
                log.error("flush error when shutdown", e);
            }
        }

        // unmap mappedByteBuffer
        UtilAll.cleanBuffer(this.mappedByteBuffer);
        UtilAll.cleanBuffer(this.byteBuffer);
        UtilAll.cleanBuffer(this.snapshotBuffer);
        localBuffer.remove();

        if (null != this.fileChannel) {
            try {
                this.fileChannel.close();
            } catch (Throwable t) {
                log.error("Shutdown error in timer wheel", t);
            }
        }
    }

    public void flush() {
        if (mappedByteBuffer == null) {
            return;
        }
        ByteBuffer bf = localBuffer.get();
        bf.position(0);
        bf.limit(wheelLength);
        mappedByteBuffer.position(0);
        mappedByteBuffer.limit(wheelLength);
        for (int i = 0; i < wheelLength; i++) {
            if (bf.get(i) != mappedByteBuffer.get(i)) {
                mappedByteBuffer.put(i, bf.get(i));
            }
        }
        this.mappedByteBuffer.force();
    }

    /*
     * Caller holds lockWhenFlush; the frozen buffer stays immutable until the next copy.
     * The first copy freezes the whole wheel; later copies only refresh slots dirtied since
     * the previous freeze, so the lock is held for the changed slots instead of all 36.9 MiB.
     */
    public void copyForSnapshot() {
        if (snapshotBuffer == null) {
            snapshotBuffer = ByteBuffer.allocateDirect(wheelLength);
        }
        if (!snapshotInitialized || dirtySlots == null) {
            copyWholeWheel();
            return;
        }
        // A dense dirty set is cheaper to copy in one sequential memcpy than slot by slot.
        if (dirtySlots.cardinality() > slotsTotal * 2 / 4) {
            copyWholeWheel();
            return;
        }
        int copied = 0;
        for (int slotIndex = dirtySlots.nextSetBit(0); slotIndex >= 0;
            slotIndex = dirtySlots.nextSetBit(slotIndex + 1)) {
            copySlot(slotIndex);
            copied++;
        }
        dirtySlots.clear();
        lastSnapshotCopiedSlots = copied;
        snapshotBuffer.position(0);
        snapshotBuffer.limit(wheelLength);
    }

    private void copyWholeWheel() {
        ByteBuffer bf = byteBuffer.duplicate();
        bf.position(0);
        bf.limit(wheelLength);
        snapshotBuffer.clear();
        snapshotBuffer.put(bf);
        snapshotBuffer.flip();
        snapshotInitialized = true;
        lastSnapshotCopiedSlots = slotsTotal * 2;
        if (dirtySlots != null) {
            dirtySlots.clear();
        }
    }

    private void copySlot(int slotIndex) {
        int position = slotIndex * Slot.SIZE;
        snapshotBuffer.putLong(position, byteBuffer.getLong(position));
        snapshotBuffer.putLong(position + 8, byteBuffer.getLong(position + 8));
        snapshotBuffer.putLong(position + 16, byteBuffer.getLong(position + 16));
        snapshotBuffer.putInt(position + 24, byteBuffer.getInt(position + 24));
        snapshotBuffer.putInt(position + 28, byteBuffer.getInt(position + 28));
    }

    private void markSlotDirty(int slotIndex) {
        if (dirtySlots != null) {
            dirtySlots.set(slotIndex);
        }
    }

    // testable
    int getLastSnapshotCopiedSlots() {
        return lastSnapshotCopiedSlots;
    }

    /* 1 MiB segments + force bound the writeback burst; tmp + atomic rename keep the snapshot complete. */
    public void backup(long flushWhere) throws IOException {
        ByteBuffer bf = snapshotBuffer;
        bf.position(0);
        bf.limit(wheelLength);

        String fileName = selectSnapshotByFlag(flushWhere);
        File bakFile = new File(fileName);
        File tmpFile = new File(fileName + ".tmp");
        Files.deleteIfExists(tmpFile.toPath());
        try (RandomAccessFile randomAccessFile = new RandomAccessFile(tmpFile, "rw")) {
            try (FileChannel fileChannel = randomAccessFile.getChannel()) {
                while (bf.hasRemaining()) {
                    bf.limit(Math.min(bf.position() + BACKUP_SEGMENT_BYTES, wheelLength));
                    while (bf.hasRemaining()) {
                        fileChannel.write(bf);
                    }
                    fileChannel.force(false);
                    bf.limit(wheelLength);
                }
                fileChannel.force(true);
            }
        }

        if (tmpFile.exists()) {
            Files.move(tmpFile.toPath(), bakFile.toPath(),
                StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
            MixAll.fsyncDirectory(Paths.get(bakFile.getParent()));
        }
        cleanExpiredSnapshot();
    }

    /* The checkpoint must not advance until replacement and directory fsync complete. */
    public void promoteToBase(long offset) throws IOException {
        File snapshot = new File(selectSnapshotByFlag(offset));
        File base = new File(this.fileName);
        Files.move(snapshot.toPath(), base.toPath(),
            StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        MixAll.fsyncDirectory(Paths.get(base.getParent()));
        cleanAllSnapshots();
    }

    /**
     * Select snapshot file name based on flag.
     *
     * @param flag Flag used to select or identify snapshot file.
     * @return Name of the snapshot file.
     */
    private String selectSnapshotByFlag(long flag) {
        if (flag < 0) {
            return this.fileName; // If flag is less than 0, return default file name
        }
        return this.fileName + "." + flag; // Otherwise, return file name with flag suffix
    }

    /**
     * Clean up expired snapshot files.
     * <p>
     * This method will find and delete all snapshot files with flags smaller than the specified value
     * under the current file name, keeping the two snapshot files with the largest flags.
     */
    public void cleanExpiredSnapshot() {
        File dir = new File(this.fileName).getParentFile();
        File[] files = dir.listFiles();
        if (files == null) {
            return;
        }

        // Collect all snapshot files and their flags
        List<FileWithFlag> snapshotFiles = new ArrayList<>();
        for (File file : files) {
            String fileName = file.getName();
            if (fileName.startsWith(TIMER_WHEEL_FILE_NAME + ".")) {
                long flag = UtilAll.asLong(fileName.substring(TIMER_WHEEL_FILE_NAME.length() + 1), -1);
                if (flag >= 0) {
                    snapshotFiles.add(new FileWithFlag(file, flag));
                }
            }
        }

        // Sort by flag in descending order
        snapshotFiles.sort((a, b) -> Long.compare(b.flag, a.flag));

        // Delete all files except the first two
        for (int i = 2; i < snapshotFiles.size(); i++) {
            UtilAll.deleteFile(snapshotFiles.get(i).file);
        }
    }

    private void cleanAllSnapshots() {
        File dir = new File(this.fileName).getParentFile();
        File[] files = dir.listFiles();
        if (files == null) {
            return;
        }
        for (File file : files) {
            String fileName = file.getName();
            if (fileName.startsWith(TIMER_WHEEL_FILE_NAME + ".")) {
                long flag = UtilAll.asLong(fileName.substring(TIMER_WHEEL_FILE_NAME.length() + 1), -1);
                if (flag >= 0) {
                    UtilAll.deleteFile(file);
                }
            }
        }
    }

    /**
     * Get the maximum flag from existing snapshot files.
     *
     * @return The maximum flag value, or -1 if no snapshot files exist
     */
    public static long getMaxSnapshotFlag(String timerWheelPath) {
        File dir = new File(timerWheelPath).getParentFile();
        File[] files = dir.listFiles();
        if (files == null) {
            return -1;
        }

        long maxFlag = -1;
        for (File file : files) {
            String fileName = file.getName();
            if (fileName.startsWith(TIMER_WHEEL_FILE_NAME + ".")) {
                long flag = UtilAll.asLong(fileName.substring(TIMER_WHEEL_FILE_NAME.length() + 1), -1);
                if (flag > maxFlag) {
                    maxFlag = flag;
                }
            }
        }
        return maxFlag;
    }

    /**
     * Wrapper class for file and flag
     */
    private static class FileWithFlag {
        final File file;
        final long flag;

        FileWithFlag(File file, long flag) {
            this.file = file;
            this.flag = flag;
        }
    }

    public Slot getSlot(long timeMs) {
        Slot slot = getRawSlot(timeMs);
        if (slot.timeMs != timeMs / precisionMs * precisionMs) {
            return new Slot(-1, -1, -1);
        }
        return slot;
    }

    //testable
    public Slot getRawSlot(long timeMs) {
        localBuffer.get().position(getSlotIndex(timeMs) * Slot.SIZE);
        return new Slot(localBuffer.get().getLong() * precisionMs,
            localBuffer.get().getLong(), localBuffer.get().getLong(), localBuffer.get().getInt(), localBuffer.get().getInt());
    }

    public int getSlotIndex(long timeMs) {
        return (int) (timeMs / precisionMs % (slotsTotal * 2));
    }

    public void putSlot(long timeMs, long firstPos, long lastPos) {
        int slotIndex = getSlotIndex(timeMs);
        localBuffer.get().position(slotIndex * Slot.SIZE);
        // To be compatible with previous version.
        // The previous version's precision is fixed at 1000ms and it store timeMs / 1000 in slot.
        localBuffer.get().putLong(timeMs / precisionMs);
        localBuffer.get().putLong(firstPos);
        localBuffer.get().putLong(lastPos);
        markSlotDirty(slotIndex);
    }

    public void putSlot(long timeMs, long firstPos, long lastPos, int num, int magic) {
        int slotIndex = getSlotIndex(timeMs);
        localBuffer.get().position(slotIndex * Slot.SIZE);
        localBuffer.get().putLong(timeMs / precisionMs);
        localBuffer.get().putLong(firstPos);
        localBuffer.get().putLong(lastPos);
        localBuffer.get().putInt(num);
        localBuffer.get().putInt(magic);
        markSlotDirty(slotIndex);
    }

    public void reviseSlot(long timeMs, long firstPos, long lastPos, boolean force) {
        int slotIndex = getSlotIndex(timeMs);
        localBuffer.get().position(slotIndex * Slot.SIZE);

        if (timeMs / precisionMs != localBuffer.get().getLong()) {
            if (force) {
                putSlot(timeMs, firstPos != IGNORE ? firstPos : lastPos, lastPos);
            }
        } else {
            if (IGNORE != firstPos) {
                localBuffer.get().putLong(firstPos);
            } else {
                localBuffer.get().getLong();
            }
            if (IGNORE != lastPos) {
                localBuffer.get().putLong(lastPos);
            }
        }
        markSlotDirty(slotIndex);
    }

    //check the timerwheel to see if its stored offset > maxOffset in timerlog
    public long checkPhyPos(long timeStartMs, long maxOffset) {
        long minFirst = Long.MAX_VALUE;
        int firstSlotIndex = getSlotIndex(timeStartMs);
        for (int i = 0; i < slotsTotal * 2; i++) {
            int slotIndex = (firstSlotIndex + i) % (slotsTotal * 2);
            localBuffer.get().position(slotIndex * Slot.SIZE);
            if ((timeStartMs + i * precisionMs) / precisionMs != localBuffer.get().getLong()) {
                continue;
            }
            long first = localBuffer.get().getLong();
            long last = localBuffer.get().getLong();
            if (last > maxOffset) {
                if (first < minFirst) {
                    minFirst = first;
                }
            }
        }
        return minFirst;
    }

    public long getNum(long timeMs) {
        return getSlot(timeMs).num;
    }

    public long getAllNum(long timeStartMs) {
        int allNum = 0;
        int firstSlotIndex = getSlotIndex(timeStartMs);
        for (int i = 0; i < slotsTotal * 2; i++) {
            int slotIndex = (firstSlotIndex + i) % (slotsTotal * 2);
            localBuffer.get().position(slotIndex * Slot.SIZE);
            if ((timeStartMs + i * precisionMs) / precisionMs == localBuffer.get().getLong()) {
                localBuffer.get().getLong(); //first pos
                localBuffer.get().getLong(); //last pos
                allNum = allNum + localBuffer.get().getInt();
            }
        }
        return allNum;
    }

    public String getFileName() {
        return fileName;
    }
}
