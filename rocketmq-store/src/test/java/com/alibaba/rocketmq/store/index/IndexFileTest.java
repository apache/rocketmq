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

/**
 * $Id: IndexFileTest.java 1831 2013-05-16 01:39:51Z shijia.wxr $
 */
package com.alibaba.rocketmq.store.index;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;


public class IndexFileTest {
    private final int hashSlotNum = 100;
    private final int indexNum = 400;


    @Test
    public void test_put_index() {
        try {
            IndexFile indexFile = new IndexFile("100", hashSlotNum, indexNum, 0, 0);

            // 写入索引
            for (long i = 0; i < (indexNum - 1); i++) {
                boolean putResult = indexFile.putKey(Long.toString(i), i, System.currentTimeMillis());
                assertTrue(putResult);
            }

            // 索引文件已经满了， 再写入会失败
            boolean putResult = indexFile.putKey(Long.toString(400), 400, System.currentTimeMillis());
            assertFalse(putResult);

            // 删除文件
            indexFile.destroy(0);
        } catch (Exception e) {
            e.printStackTrace();
            assertTrue(false);
        }
    }


    @Test
    public void test_put_get_index() {
        try {
            IndexFile indexFile = new IndexFile("200", hashSlotNum, indexNum, 0, 0);

            // 写入索引
            for (long i = 0; i < (indexNum - 1); i++) {
                boolean putResult = indexFile.putKey(Long.toString(i), i, System.currentTimeMillis());
                assertTrue(putResult);
            }

            // 索引文件已经满了， 再写入会失败
            boolean putResult = indexFile.putKey(Long.toString(400), 400, System.currentTimeMillis());
            assertFalse(putResult);

            // 读索引
            final List<Long> phyOffsets = new ArrayList<Long>();
            indexFile.selectPhyOffset(phyOffsets, "60", 10, 0, Long.MAX_VALUE, true);
            for (Long offset : phyOffsets) {
                System.out.println(offset);
            }

            assertFalse(phyOffsets.isEmpty());

            // 删除文件
            indexFile.destroy(0);
        } catch (Exception e) {
            e.printStackTrace();
            assertTrue(false);
        }
    }
}
