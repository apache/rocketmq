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
package org.apache.rocketmq.common;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.Map;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

public abstract class ConfigManager {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.COMMON_LOGGER_NAME);

    public boolean load() {
        String fileName = null;
        try {
            fileName = this.configFilePath();
            String jsonString = MixAll.file2String(fileName);

            if (null == jsonString || jsonString.length() == 0) {
                // delete invalid file
                Files.deleteIfExists(Paths.get(fileName));
                return this.loadBak();
            } else {
                this.decode(jsonString);
                log.info("load " + fileName + " OK");
                return true;
            }
        } catch (Exception e) {
            log.error("load " + fileName + " failed, and try to load backup file", e);
            try {
                if (fileName != null) {
                    // delete invalid file
                    Files.deleteIfExists(Paths.get(fileName));
                }
            } catch (Throwable t) {
                log.error("load " + fileName + " failed, and delete invalid file errr", e);
            }
            return this.loadBak();
        }
    }

    private boolean loadBak() {
        String fileName = null;
        try {
            fileName = this.configFilePath() + ".bak";
            String jsonString = MixAll.file2String(fileName);
            if (jsonString != null && jsonString.length() > 0) {
                this.decode(jsonString);
                log.info("load " + fileName + " OK");
                return true;
            }
        } catch (Exception e) {
            log.error("load " + fileName + " Failed", e);
            return false;
        }

        return true;
    }

    public synchronized <T> void persist(String topicName, T t) {
        // stub for future
        this.persist();
    }

    public synchronized <T> void persist(Map<String, T> m) {
        // stub for future
        this.persist();
    }

    public synchronized void persist() {
        String jsonString = this.encode(true);
        if (jsonString != null) {
            try {
                // bak metrics file
                String config = configFilePath();
                String backup = config + ".bak";
                File configFile = new File(config);
                File bakFile = new File(backup);

                if (configFile.exists()) {
                    // atomic move
                    Files.move(configFile.toPath(), bakFile.toPath(), StandardCopyOption.ATOMIC_MOVE);

                    // sync the directory, ensure that the bak file is visible
                    MixAll.fsyncDirectory(Paths.get(bakFile.getParent()));
                }

                File dir = new File(Path.of(config).getParent().toString());
                if (!dir.exists()) {
                    Files.createDirectories(dir.toPath());
                }

                try (RandomAccessFile randomAccessFile = new RandomAccessFile(config, "rw")) {
                    randomAccessFile.write(jsonString.getBytes(StandardCharsets.UTF_8));
                    randomAccessFile.getChannel().force(true);
                    // sync the directory, ensure that the config file is visible
                    MixAll.fsyncDirectory(Paths.get(configFile.getParent()));
                }
            } catch (Throwable t) {
                log.error("Failed to persist", t);
            }
        }
    }

    public boolean stop() {
        return true;
    }

    public void shutdown() {
        stop();
    }

    public abstract String configFilePath();

    public abstract String encode();

    public abstract String encode(final boolean prettyFormat);

    public abstract void decode(final String jsonString);
}
