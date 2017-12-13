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

package org.apache.rocketmq.srvutil;

import com.google.common.hash.HashCode;
import com.google.common.hash.Hashing;
import com.google.common.io.Files;
import java.io.File;
import java.io.IOException;
import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.constant.LoggerName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileWatchService extends ServiceThread {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.COMMON_LOGGER_NAME);

    private String [] watchFiles;
    private boolean [] isFileChangedFlag;
    private HashCode [] fileCurrentHash;
    private Listener listener;
    private static final int WATCH_INTERVAL = 100;


    public FileWatchService(final String [] watchFiles,
        final Listener listener) throws IOException {
        this.watchFiles = watchFiles;
        this.listener = listener;
        this.isFileChangedFlag = new boolean[watchFiles.length];
        this.fileCurrentHash = new HashCode[watchFiles.length];

        for (int i = 0; i < watchFiles.length; i++) {
            isFileChangedFlag[i] = false;
            fileCurrentHash[i] = Files.hash(new File(watchFiles[i]), Hashing.md5());
        }
    }

    @Override
    public String getServiceName() {
        return "FileWatchService";
    }

    @Override
    public void run() {
        log.info(this.getServiceName() + " service started");

        while (!this.isStopped()) {
            try {
                this.waitForRunning(WATCH_INTERVAL);

                boolean allFileChanged = true;
                for (int i = 0; i < watchFiles.length; i++) {
                    HashCode newHash = Files.hash(new File(watchFiles[i]), Hashing.md5());
                    if (!newHash.equals(fileCurrentHash[i])) {
                        isFileChangedFlag[i] = true;
                        fileCurrentHash[i] = newHash;
                    }
                    allFileChanged = allFileChanged && isFileChangedFlag[i];
                }

                if (allFileChanged) {
                    listener.onChanged();
                    for (int i = 0; i < isFileChangedFlag.length; i++) {
                        isFileChangedFlag[i] = false;
                    }
                }
            } catch (Exception e) {
                log.warn(this.getServiceName() + " service has exception. ", e);
            }
        }
        log.info(this.getServiceName() + " service end");
    }

    public interface Listener {
        /**
         * Will be called when the target files are changed
         */
        void onChanged();
    }
}
