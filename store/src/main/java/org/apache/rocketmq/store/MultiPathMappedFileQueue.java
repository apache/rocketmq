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


import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Supplier;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.store.config.MessageStoreConfig;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class MultiPathMappedFileQueue extends MappedFileQueue {

    private final MessageStoreConfig config;
    private final Supplier<Set<String>> fullStorePathsSupplier;

    public MultiPathMappedFileQueue(MessageStoreConfig messageStoreConfig, int mappedFileSize,
                                    AllocateMappedFileService allocateMappedFileService,
                                    Supplier<Set<String>> fullStorePathsSupplier) {
        super(messageStoreConfig.getStorePathCommitLog(), mappedFileSize, allocateMappedFileService);
        this.config = messageStoreConfig;
        this.fullStorePathsSupplier = fullStorePathsSupplier;
    }

    private Set<String> getPaths() {
        String[] paths = config.getStorePathCommitLog().trim().split(MessageStoreConfig.MULTI_PATH_SPLITTER);
        return new HashSet<>(Arrays.asList(paths));
    }

    private Set<String> getReadonlyPaths() {
        String pathStr = config.getReadOnlyCommitLogStorePaths();
        if (StringUtils.isBlank(pathStr)) {
            return Collections.emptySet();
        }
        String[] paths = pathStr.trim().split(MessageStoreConfig.MULTI_PATH_SPLITTER);
        return new HashSet<>(Arrays.asList(paths));
    }

    @Override
    public boolean load() {
        Set<String> storePathSet = getPaths();
        storePathSet.addAll(getReadonlyPaths());

        List<File> files = new ArrayList<>();
        for (String path : storePathSet) {
            File dir = new File(path);
            File[] ls = dir.listFiles();
            if (ls != null) {
                Collections.addAll(files, ls);
            }
        }

        return doLoad(files);
    }

    @Override
    protected MappedFile tryCreateMappedFile(long createOffset) {
        long fileIdx = createOffset / this.mappedFileSize;
        Set<String> storePath = getPaths();
        Set<String> readonlyPathSet = getReadonlyPaths();
        Set<String> fullStorePaths =
                fullStorePathsSupplier == null ? Collections.emptySet() : fullStorePathsSupplier.get();


        HashSet<String> availableStorePath = new HashSet<>(storePath);
        //do not create file in readonly store path.
        availableStorePath.removeAll(readonlyPathSet);

        //do not create file is space is nearly full.
        availableStorePath.removeAll(fullStorePaths);

        //if no store path left, fall back to writable store path.
        if (availableStorePath.isEmpty()) {
            availableStorePath = new HashSet<>(storePath);
            availableStorePath.removeAll(readonlyPathSet);
        }

        String[] paths = availableStorePath.toArray(new String[]{});
        Arrays.sort(paths);
        String nextFilePath = paths[(int) (fileIdx % paths.length)] + File.separator
                + UtilAll.offset2FileName(createOffset);
        String nextNextFilePath = paths[(int) ((fileIdx + 1) % paths.length)] + File.separator
                + UtilAll.offset2FileName(createOffset + this.mappedFileSize);
        return doCreateMappedFile(nextFilePath, nextNextFilePath);
    }

    @Override
    public void destroy() {
        for (MappedFile mf : this.mappedFiles) {
            mf.destroy(1000 * 3);
        }
        this.mappedFiles.clear();
        this.flushedWhere = 0;


        Set<String> storePathSet = getPaths();
        storePathSet.addAll(getReadonlyPaths());

        for (String path : storePathSet) {
            File file = new File(path);
            if (file.isDirectory()) {
                file.delete();
            }
        }
    }
}
