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

package org.apache.rocketmq.store.ha.autoswitch;

import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.UtilAll;

import java.io.File;

public abstract class MetadataFile {

    protected String filePath;

    public abstract String encodeToStr();

    public abstract void decodeFromStr(String dataStr);

    public abstract boolean isLoaded();

    public abstract void clearInMem();

    public void writeToFile() throws Exception {
        UtilAll.deleteFile(new File(filePath));
        MixAll.string2File(encodeToStr(), this.filePath);
    }

    public void readFromFile() throws Exception {
        String dataStr = MixAll.file2String(filePath);
        decodeFromStr(dataStr);
    }
    public boolean fileExists() {
        File file = new File(filePath);
        return file.exists();
    }

    public void clear() {
        clearInMem();
        UtilAll.deleteFile(new File(filePath));
    }

    public String getFilePath() {
        return filePath;
    }
}
