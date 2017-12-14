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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileWatchService extends ServiceThread {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.COMMON_LOGGER_NAME);

    private final String [] watchFiles;
    private final boolean [] isFileChangedFlag;
    private final String [] fileCurrentHash;
    private final Listener listener;
    private static final int WATCH_INTERVAL = 500;
    private MessageDigest md = MessageDigest.getInstance("MD5");

    public FileWatchService(final String [] watchFiles,
        final Listener listener) throws Exception {
        this.watchFiles = watchFiles;
        this.listener = listener;
        this.isFileChangedFlag = new boolean[watchFiles.length];
        this.fileCurrentHash = new String[watchFiles.length];

        for (int i = 0; i < watchFiles.length; i++) {
            isFileChangedFlag[i] = false;
            fileCurrentHash[i] = hash(watchFiles[i]);
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
                    String newHash = hash(watchFiles[i]);
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

    private String hash(String filePath) throws IOException, NoSuchAlgorithmException {
        Path path = Paths.get(filePath);
        md.update(Files.readAllBytes(path));
        byte[] hash = md.digest();
        return UtilAll.bytes2string(hash);
    }

    public interface Listener {
        /**
         * Will be called when the target files are changed
         */
        void onChanged();
    }
}
