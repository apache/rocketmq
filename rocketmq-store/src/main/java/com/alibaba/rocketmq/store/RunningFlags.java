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

/**
 * @author shijia.wxr
 */
public class RunningFlags {

    private static final int NotReadableBit = 1;

    private static final int NotWriteableBit = 1 << 1;

    private static final int WriteLogicsQueueErrorBit = 1 << 2;

    private static final int WriteIndexFileErrorBit = 1 << 3;

    private static final int DiskFullBit = 1 << 4;
    private volatile int flagBits = 0;


    public RunningFlags() {
    }


    public int getFlagBits() {
        return flagBits;
    }


    public boolean getAndMakeReadable() {
        boolean result = this.isReadable();
        if (!result) {
            this.flagBits &= ~NotReadableBit;
        }
        return result;
    }


    public boolean isReadable() {
        if ((this.flagBits & NotReadableBit) == 0) {
            return true;
        }

        return false;
    }


    public boolean getAndMakeNotReadable() {
        boolean result = this.isReadable();
        if (result) {
            this.flagBits |= NotReadableBit;
        }
        return result;
    }


    public boolean getAndMakeWriteable() {
        boolean result = this.isWriteable();
        if (!result) {
            this.flagBits &= ~NotWriteableBit;
        }
        return result;
    }


    public boolean isWriteable() {
        if ((this.flagBits & (NotWriteableBit | WriteLogicsQueueErrorBit | DiskFullBit | WriteIndexFileErrorBit)) == 0) {
            return true;
        }

        return false;
    }


    public boolean getAndMakeNotWriteable() {
        boolean result = this.isWriteable();
        if (result) {
            this.flagBits |= NotWriteableBit;
        }
        return result;
    }


    public void makeLogicsQueueError() {
        this.flagBits |= WriteLogicsQueueErrorBit;
    }


    public boolean isLogicsQueueError() {
        if ((this.flagBits & WriteLogicsQueueErrorBit) == WriteLogicsQueueErrorBit) {
            return true;
        }

        return false;
    }


    public void makeIndexFileError() {
        this.flagBits |= WriteIndexFileErrorBit;
    }


    public boolean isIndexFileError() {
        if ((this.flagBits & WriteIndexFileErrorBit) == WriteIndexFileErrorBit) {
            return true;
        }

        return false;
    }


    public boolean getAndMakeDiskFull() {
        boolean result = !((this.flagBits & DiskFullBit) == DiskFullBit);
        this.flagBits |= DiskFullBit;
        return result;
    }


    public boolean getAndMakeDiskOK() {
        boolean result = !((this.flagBits & DiskFullBit) == DiskFullBit);
        this.flagBits &= ~DiskFullBit;
        return result;
    }
}
