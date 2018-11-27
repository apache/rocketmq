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

package org.apache.rocketmq.store.dleger;

import io.openmessaging.storage.dleger.store.file.SelectMmapBufferResult;
import org.apache.rocketmq.store.SelectMappedBufferResult;

public class DLegerSelectMappedBufferResult extends SelectMappedBufferResult {

    private SelectMmapBufferResult sbr;
    public DLegerSelectMappedBufferResult(SelectMmapBufferResult sbr) {
        super(sbr.getStartOffset(), sbr.getByteBuffer(), sbr.getSize(), null);
        this.sbr = sbr;
    }

    @Override
    public synchronized void release() {
        super.release();
        sbr.release();
    }
}
