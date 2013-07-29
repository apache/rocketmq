/**
 * Copyright (C) 2010-2013 Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.rocketmq.store;

import java.nio.ByteBuffer;


/**
 * 查询Pagecache返回结果
 * 
 * @author shijia.wxr<vintage.wang@gmail.com>
 * @since 2013-7-21
 */
public class SelectMapedBufferResult {
    // 从队列中哪个绝对Offset开始
    private final long startOffset;
    // position从0开始
    private final ByteBuffer byteBuffer;
    // 有效数据大小
    private int size;
    // 用来释放内存
    private MapedFile mapedFile;


    public SelectMapedBufferResult(long startOffset, ByteBuffer byteBuffer, int size, MapedFile mapedFile) {
        this.startOffset = startOffset;
        this.byteBuffer = byteBuffer;
        this.size = size;
        this.mapedFile = mapedFile;
    }


    public ByteBuffer getByteBuffer() {
        return byteBuffer;
    }


    public int getSize() {
        return size;
    }


    public void setSize(final int s) {
        this.size = s;
        this.byteBuffer.limit(this.size);
    }


    public MapedFile getMapedFile() {
        return mapedFile;
    }


    @Override
    protected void finalize() {
        if (this.mapedFile != null) {
            this.release();
        }
    }


    /**
     * 此方法只能被调用一次，重复调用无效
     */
    public synchronized void release() {
        if (this.mapedFile != null) {
            this.mapedFile.release();
            this.mapedFile = null;
        }
    }


    public long getStartOffset() {
        return startOffset;
    }
}
