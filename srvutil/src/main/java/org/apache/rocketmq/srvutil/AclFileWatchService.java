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

import java.util.ArrayList;
import java.util.List;
import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.util.HashMap;
import java.util.Map;

public class AclFileWatchService extends ServiceThread {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.COMMON_LOGGER_NAME);

    private final String aclPath;
    private int aclFilesNum;
    @Deprecated
    private final Map<String, String> fileCurrentHash;
    private Map<String, Long> fileLastModifiedTime;
    private List<String/**absolute pathname **/> fileList = new ArrayList<>();
    private final AclFileWatchService.Listener listener;
    private static final int WATCH_INTERVAL = 5000;
    private MessageDigest md = MessageDigest.getInstance("MD5");
    private String defaultAclFile;

    public AclFileWatchService(String path, String defaultAclFile, final AclFileWatchService.Listener listener) throws Exception {
        this.aclPath = path;
        this.defaultAclFile = defaultAclFile;
        this.fileCurrentHash = new HashMap<>();
        this.fileLastModifiedTime = new HashMap<>();
        this.listener = listener;

        getAllAclFiles(path);
        if (new File(this.defaultAclFile).exists() && !fileList.contains(this.defaultAclFile)) {
            fileList.add(this.defaultAclFile);
        }
        this.aclFilesNum = fileList.size();
        for (int i = 0; i < aclFilesNum; i++) {
            String fileAbsolutePath = fileList.get(i);
            this.fileLastModifiedTime.put(fileAbsolutePath, new File(fileAbsolutePath).lastModified());
        }

    }

    public void getAllAclFiles(String path) {
        File file = new File(path);
        if (!file.exists()) {
            log.info("The default acl dir {} is not exist", path);
            return;
        }
        File[] files = file.listFiles();
        for (int i = 0; i < files.length; i++) {
            String fileName = files[i].getAbsolutePath();
            File f = new File(fileName);
            if (fileName.equals(aclPath + File.separator + "tools.yml")) {
                continue;
            } else if (fileName.endsWith(".yml") || fileName.endsWith(".yaml")) {
                fileList.add(fileName);
            } else if (f.isDirectory()) {
                getAllAclFiles(fileName);
            }
        }
    }

    @Override
    public String getServiceName() {
        return "AclFileWatchService";
    }

    @Override
    public void run() {
        log.info(this.getServiceName() + " service started");

        while (!this.isStopped()) {
            try {
                this.waitForRunning(WATCH_INTERVAL);

                if (fileList.size() > 0) {
                    fileList.clear();
                }
                getAllAclFiles(aclPath);
                if (new File(defaultAclFile).exists() && !fileList.contains(defaultAclFile)) {
                    fileList.add(defaultAclFile);
                }
                int realAclFilesNum = fileList.size();

                if (aclFilesNum != realAclFilesNum) {
                    log.info("aclFilesNum: " + aclFilesNum + "  realAclFilesNum: " + realAclFilesNum);
                    aclFilesNum = realAclFilesNum;
                    log.info("aclFilesNum: " + aclFilesNum + "  realAclFilesNum: " + realAclFilesNum);
                    Map<String, Long> fileLastModifiedTime = new HashMap<>(realAclFilesNum);
                    for (int i = 0; i < realAclFilesNum; i++) {
                        String fileAbsolutePath = fileList.get(i);
                        fileLastModifiedTime.put(fileAbsolutePath, new File(fileAbsolutePath).lastModified());
                    }
                    this.fileLastModifiedTime = fileLastModifiedTime;
                    listener.onFileNumChanged(aclPath);
                } else {
                    for (int i = 0; i < aclFilesNum; i++) {
                        String fileName = fileList.get(i);
                        Long newLastModifiedTime = new File(fileName).lastModified();
                        if (!newLastModifiedTime.equals(fileLastModifiedTime.get(fileName))) {
                            fileLastModifiedTime.put(fileName, newLastModifiedTime);
                            listener.onFileChanged(fileName);
                        }
                    }
                }
            } catch (Exception e) {
                log.warn(this.getServiceName() + " service has exception. ", e);
            }
        }
        log.info(this.getServiceName() + " service end");
    }

    @Deprecated
    private String hash(String filePath) throws IOException {
        Path path = Paths.get(filePath);
        md.update(Files.readAllBytes(path));
        byte[] hash = md.digest();
        return UtilAll.bytes2string(hash);
    }

    public interface Listener {
        /**
         * Will be called when the target file is changed
         *
         * @param aclFileName the changed file absolute path
         */
        void onFileChanged(String aclFileName);

        /**
         * Will be called when the number of the acl file is changed
         *
         * @param path the path of the acl dir
         */
        void onFileNumChanged(String path);
    }
}
