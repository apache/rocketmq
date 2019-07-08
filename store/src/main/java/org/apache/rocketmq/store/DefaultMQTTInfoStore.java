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
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

public class DefaultMQTTInfoStore implements MQTTInfoStore {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);
    private RocksDB db;
    private String storePathRootDir = System.getProperty("user.home") + File.separator + "store";
    private String storePathRocksDB = storePathRootDir
        + File.separator + "RocksDB";

    @Override public void load() {
        RocksDB.loadLibrary();
    }

    @Override public void start() throws Exception {

        try (final Options options = new Options().setCreateIfMissing(true)) {
            if (!Files.isSymbolicLink(Paths.get(storePathRocksDB))) {
                Files.createDirectories(Paths.get(storePathRocksDB));
            }
            db = RocksDB.open(options, storePathRocksDB);
        } catch (RocksDBException e) {
            log.error("Open RocksDb failed. Error:{}", e);
            throw e;
        }

    }

    @Override public boolean putData(String key, String value) {
        try {
            db.put(key.getBytes(), value.getBytes());
            return true;
        } catch (Exception e) {
            log.error("RocksDB put data failed. Error:{}", e);
            return false;
        }
    }

    @Override public String getValue(String key) {
        try {
            byte[] value = db.get(key.getBytes());
            if (value != null) {
                return new String(value, Charset.forName("UTF-8"));
            } else {
                return null;
            }
        } catch (Exception e) {
            log.error("RocksDB get value failed. Error:{}", e);
            return null;
        }
    }

    @Override public boolean deleteData(String key) {
        boolean result = false;
        try {
            db.delete(key.getBytes());
            result = true;
        } catch (Exception e) {
            log.error("RocksDB delete data failed. Error:{}", e);
        }
        return result;
    }
}
