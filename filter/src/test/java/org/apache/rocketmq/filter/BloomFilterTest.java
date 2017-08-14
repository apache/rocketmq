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

package org.apache.rocketmq.filter;

import org.apache.rocketmq.filter.util.BitsArray;
import org.apache.rocketmq.filter.util.BloomFilter;
import org.apache.rocketmq.filter.util.BloomFilterData;
import org.junit.Test;

import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

public class BloomFilterTest {

    @Test
    public void testEquals() {
        BloomFilter a = BloomFilter.createByFn(10, 20);

        BloomFilter b = BloomFilter.createByFn(10, 20);

        BloomFilter c = BloomFilter.createByFn(12, 20);

        BloomFilter d = BloomFilter.createByFn(10, 30);

        assertThat(a).isEqualTo(b);
        assertThat(a).isNotEqualTo(c);
        assertThat(a).isNotEqualTo(d);
        assertThat(d).isNotEqualTo(c);

        assertThat(a.hashCode()).isEqualTo(b.hashCode());
        assertThat(a.hashCode()).isNotEqualTo(c.hashCode());
        assertThat(a.hashCode()).isNotEqualTo(d.hashCode());
        assertThat(c.hashCode()).isNotEqualTo(d.hashCode());
    }

    @Test
    public void testHashTo() {
        String cid = "CID_abc_efg";

        BloomFilter bloomFilter = BloomFilter.createByFn(10, 20);

        BitsArray bits = BitsArray.create(bloomFilter.getM());

        int[] bitPos = bloomFilter.calcBitPositions(cid);

        bloomFilter.hashTo(cid, bits);

        for (int bit : bitPos) {
            assertThat(bits.getBit(bit)).isTrue();
        }
    }

    @Test
    public void testCalcBitPositions() {
        String cid = "CID_abc_efg";

        BloomFilter bloomFilter = BloomFilter.createByFn(10, 20);

        int[] bitPos = bloomFilter.calcBitPositions(cid);

        assertThat(bitPos).isNotNull();
        assertThat(bitPos.length).isEqualTo(bloomFilter.getK());

        int[] bitPos2 = bloomFilter.calcBitPositions(cid);

        assertThat(bitPos2).isNotNull();
        assertThat(bitPos2.length).isEqualTo(bloomFilter.getK());

        assertThat(bitPos).isEqualTo(bitPos2);
    }

    @Test
    public void testIsHit() {
        String cid = "CID_abc_efg";
        String cid2 = "CID_abc_123";

        BloomFilter bloomFilter = BloomFilter.createByFn(10, 20);

        BitsArray bits = BitsArray.create(bloomFilter.getM());

        bloomFilter.hashTo(cid, bits);

        assertThat(bloomFilter.isHit(cid, bits)).isTrue();
        assertThat(!bloomFilter.isHit(cid2, bits)).isTrue();

        bloomFilter.hashTo(cid2, bits);

        assertThat(bloomFilter.isHit(cid, bits)).isTrue();
        assertThat(bloomFilter.isHit(cid2, bits)).isTrue();
    }

    @Test
    public void testBloomFilterData() {
        BloomFilterData bloomFilterData = new BloomFilterData(new int[] {1, 2, 3}, 128);
        BloomFilterData bloomFilterData1 = new BloomFilterData(new int[] {1, 2, 3}, 128);
        BloomFilterData bloomFilterData2 = new BloomFilterData(new int[] {1, 2, 3}, 129);

        assertThat(bloomFilterData).isEqualTo(bloomFilterData1);
        assertThat(bloomFilterData2).isNotEqualTo(bloomFilterData);
        assertThat(bloomFilterData2).isNotEqualTo(bloomFilterData1);

        assertThat(bloomFilterData.hashCode()).isEqualTo(bloomFilterData1.hashCode());
        assertThat(bloomFilterData2.hashCode()).isNotEqualTo(bloomFilterData.hashCode());
        assertThat(bloomFilterData2.hashCode()).isNotEqualTo(bloomFilterData1.hashCode());

        assertThat(bloomFilterData.getBitPos()).isEqualTo(bloomFilterData2.getBitPos());
        assertThat(bloomFilterData.getBitNum()).isEqualTo(bloomFilterData1.getBitNum());
        assertThat(bloomFilterData.getBitNum()).isNotEqualTo(bloomFilterData2.getBitNum());

        bloomFilterData2.setBitNum(128);

        assertThat(bloomFilterData).isEqualTo(bloomFilterData2);

        bloomFilterData2.setBitPos(new int[] {1, 2, 3, 4});

        assertThat(bloomFilterData).isNotEqualTo(bloomFilterData2);

        BloomFilterData nullData = new BloomFilterData();

        assertThat(nullData.getBitNum()).isEqualTo(0);
        assertThat(nullData.getBitPos()).isNull();

        BloomFilter bloomFilter = BloomFilter.createByFn(1, 300);

        assertThat(bloomFilter).isNotNull();
        assertThat(bloomFilter.isValid(bloomFilterData)).isFalse();
    }

    @Test
    public void testCheckFalseHit() {
        BloomFilter bloomFilter = BloomFilter.createByFn(1, 300);
        BitsArray bits = BitsArray.create(bloomFilter.getM());
        int falseHit = 0;
        for (int i = 0; i < bloomFilter.getN(); i++) {
            String str = randomString((new Random(System.nanoTime())).nextInt(127) + 10);
            int[] bitPos = bloomFilter.calcBitPositions(str);

            if (bloomFilter.checkFalseHit(bitPos, bits)) {
                falseHit++;
            }

            bloomFilter.hashTo(bitPos, bits);
        }

        assertThat(falseHit).isLessThanOrEqualTo(bloomFilter.getF() * bloomFilter.getN() / 100);
    }

    private String randomString(int length) {
        StringBuilder stringBuilder = new StringBuilder(length);
        for (int i = 0; i < length; i++) {
            stringBuilder.append((char) ((new Random(System.nanoTime())).nextInt(123 - 97) + 97));
        }

        return stringBuilder.toString();
    }
}
