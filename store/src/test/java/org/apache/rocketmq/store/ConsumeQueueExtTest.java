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

import java.io.File;
import java.util.Random;
import org.apache.rocketmq.common.UtilAll;
import org.junit.After;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class ConsumeQueueExtTest {

    private static final String topic = "abc";
    private static final int queueId = 0;
    private static final String storePath = System.getProperty("java.io.tmpdir") + File.separator + "unit_test_store";
    private static final int bitMapLength = 64;
    private static final int unitSizeWithBitMap = ConsumeQueueExt.CqExtUnit.MIN_EXT_UNIT_SIZE + bitMapLength / Byte.SIZE;
    private static final int cqExtFileSize = 10 * unitSizeWithBitMap;
    private static final int unitCount = 20;

    protected ConsumeQueueExt genExt() {
        return new ConsumeQueueExt(
            topic, queueId, storePath, cqExtFileSize, bitMapLength
        );
    }

    protected byte[] genBitMap(int bitMapLength) {
        byte[] bytes = new byte[bitMapLength / Byte.SIZE];

        Random random = new Random(System.currentTimeMillis());
        random.nextBytes(bytes);

        return bytes;
    }

    protected ConsumeQueueExt.CqExtUnit genUnit(boolean hasBitMap) {
        ConsumeQueueExt.CqExtUnit cqExtUnit = new ConsumeQueueExt.CqExtUnit();

        cqExtUnit.setTagsCode(Math.abs((new Random(System.currentTimeMillis())).nextInt()));
        cqExtUnit.setMsgStoreTime(System.currentTimeMillis());
        if (hasBitMap) {
            cqExtUnit.setFilterBitMap(genBitMap(bitMapLength));
        }

        return cqExtUnit;
    }

    protected void putSth(ConsumeQueueExt consumeQueueExt, boolean getAfterPut,
        boolean unitSameSize, int unitCount) {
        for (int i = 0; i < unitCount; i++) {
            ConsumeQueueExt.CqExtUnit putUnit =
                unitSameSize ? genUnit(true) : genUnit(i % 2 == 0);

            long addr = consumeQueueExt.put(putUnit);
            assertThat(addr).isLessThan(0);

            if (getAfterPut) {
                ConsumeQueueExt.CqExtUnit getUnit = consumeQueueExt.get(addr);

                assertThat(getUnit).isNotNull();
                assertThat(putUnit).isEqualTo(getUnit);
            }

            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                e.printStackTrace();
                assertThat(false).isTrue();
            }
        }
    }

    @Test
    public void testPut() {
        ConsumeQueueExt consumeQueueExt = genExt();

        try {
            putSth(consumeQueueExt, true, false, unitCount);
        } finally {
            consumeQueueExt.destroy();
            UtilAll.deleteFile(new File(storePath));
        }
    }

    @Test
    public void testGet() {
        ConsumeQueueExt consumeQueueExt = genExt();

        putSth(consumeQueueExt, false, false, unitCount);

        try {
            // from start.
            long addr = consumeQueueExt.decorate(0);

            ConsumeQueueExt.CqExtUnit unit = new ConsumeQueueExt.CqExtUnit();
            while (true) {
                boolean ret = consumeQueueExt.get(addr, unit);

                if (!ret) {
                    break;
                }

                assertThat(unit.getSize()).isGreaterThanOrEqualTo(ConsumeQueueExt.CqExtUnit.MIN_EXT_UNIT_SIZE);

                addr += unit.getSize();
            }
        } finally {
            consumeQueueExt.destroy();
            UtilAll.deleteFile(new File(storePath));
        }
    }

    @Test
    public void testGet_invalidAddress() {
        ConsumeQueueExt consumeQueueExt = genExt();

        putSth(consumeQueueExt, false, true, unitCount);

        try {
            ConsumeQueueExt.CqExtUnit unit = consumeQueueExt.get(0);

            assertThat(unit).isNull();

            long addr = (cqExtFileSize / unitSizeWithBitMap) * unitSizeWithBitMap;
            addr += unitSizeWithBitMap;

            unit = consumeQueueExt.get(addr);
            assertThat(unit).isNull();
        } finally {
            consumeQueueExt.destroy();
            UtilAll.deleteFile(new File(storePath));
        }
    }

    @Test
    public void testRecovery() {
        ConsumeQueueExt putCqExt = genExt();

        putSth(putCqExt, false, true, unitCount);

        ConsumeQueueExt loadCqExt = genExt();

        loadCqExt.load();

        loadCqExt.recover();

        try {
            assertThat(loadCqExt.getMinAddress()).isEqualTo(Long.MIN_VALUE);

            // same unit size.
            int countPerFile = (cqExtFileSize - ConsumeQueueExt.END_BLANK_DATA_LENGTH) / unitSizeWithBitMap;

            int lastFileUnitCount = unitCount % countPerFile;

            int fileCount = unitCount / countPerFile + 1;
            if (lastFileUnitCount == 0) {
                fileCount -= 1;
            }

            if (lastFileUnitCount == 0) {
                assertThat(loadCqExt.unDecorate(loadCqExt.getMaxAddress()) % cqExtFileSize).isEqualTo(0);
            } else {
                assertThat(loadCqExt.unDecorate(loadCqExt.getMaxAddress()))
                    .isEqualTo(lastFileUnitCount * unitSizeWithBitMap + (fileCount - 1) * cqExtFileSize);
            }
        } finally {
            putCqExt.destroy();
            loadCqExt.destroy();
            UtilAll.deleteFile(new File(storePath));
        }
    }

    @Test
    public void testTruncateByMinOffset() {
        ConsumeQueueExt consumeQueueExt = genExt();

        putSth(consumeQueueExt, false, true, unitCount * 2);

        try {
            // truncate first one file.
            long address = consumeQueueExt.decorate((long) (cqExtFileSize * 1.5));

            long expectMinAddress = consumeQueueExt.decorate(cqExtFileSize);

            consumeQueueExt.truncateByMinAddress(address);

            long minAddress = consumeQueueExt.getMinAddress();

            assertThat(expectMinAddress).isEqualTo(minAddress);
        } finally {
            consumeQueueExt.destroy();
            UtilAll.deleteFile(new File(storePath));
        }
    }

    @Test
    public void testTruncateByMaxOffset() {
        ConsumeQueueExt consumeQueueExt = genExt();

        putSth(consumeQueueExt, false, true, unitCount * 2);

        try {
            // truncate, only first 3 files exist.
            long address = consumeQueueExt.decorate(cqExtFileSize * 2 + unitSizeWithBitMap);

            long expectMaxAddress = address + unitSizeWithBitMap;

            consumeQueueExt.truncateByMaxAddress(address);

            long maxAddress = consumeQueueExt.getMaxAddress();

            assertThat(expectMaxAddress).isEqualTo(maxAddress);
        } finally {
            consumeQueueExt.destroy();
            UtilAll.deleteFile(new File(storePath));
        }
    }

    @After
    public void destroy() {
        UtilAll.deleteFile(new File(storePath));
    }
}
