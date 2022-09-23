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

package org.apache.rocketmq.acl.plain;

import com.google.common.base.Preconditions;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.util.UUID;
import org.junit.Assert;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;

public final class AclTestHelper {
    private AclTestHelper() {
    }

    private static void copyTo(String path, InputStream src, File dstDir, String flag, boolean into)
        throws IOException {
        Preconditions.checkNotNull(flag);
        String[] folders = path.split(File.separator);
        boolean found = false;
        File dir = dstDir;
        for (int i = 0; i < folders.length; i++) {
            if (!found && flag.equals(folders[i])) {
                found = true;
                if (into) {
                    dir = new File(dir, flag);
                    if (!dir.exists()) {
                        Assert.assertTrue(dir.mkdirs());
                    }
                }
                continue;
            }

            if (found) {
                if (i == folders.length - 1) {
                    dir = new File(dir, folders[i]);
                } else {
                    dir = new File(dir, folders[i]);
                    if (!dir.exists()) {
                        Assert.assertTrue(dir.mkdir());
                    }
                }
            }
        }

        Assert.assertTrue(dir.createNewFile());
        byte[] buffer = new byte[4096];
        BufferedInputStream bis = new BufferedInputStream(src);
        int len = 0;
        try (BufferedOutputStream bos = new BufferedOutputStream(Files.newOutputStream(dir.toPath()))) {
            while ((len = bis.read(buffer)) > 0) {
                bos.write(buffer, 0, len);
            }
        }
    }

    public static void recursiveDelete(File file) {
        if (file.isFile()) {
            Assert.assertTrue(file.delete());
            return;
        }
        File[] files = file.listFiles();
        if (null != files) {
            for (File f : files) {
                recursiveDelete(f);
            }
        }
        Assert.assertTrue(file.delete());
    }

    public static File copyResources(String folder) throws IOException {
        return copyResources(folder, false);
    }

    public static File copyResources(String folder, boolean into) throws IOException {
        File home = new File(System.getProperty("java.io.tmpdir"), UUID.randomUUID().toString().replace('-', '_'));
        if (!home.exists()) {
            Assert.assertTrue(home.mkdirs());
        }
        PathMatchingResourcePatternResolver resolver = new PathMatchingResourcePatternResolver(AclTestHelper.class.getClassLoader());
        Resource[] resources = resolver.getResources(String.format("classpath:%s/**/*", folder));
        for (Resource resource : resources) {
            if (!resource.isReadable()) {
                continue;
            }
            String description = resource.getDescription();
            int start = description.indexOf('[');
            int end = description.lastIndexOf(']');
            String path = description.substring(start + 1, end);
            try (InputStream inputStream = resource.getInputStream()) {
                copyTo(path, inputStream, home, folder, into);
            }
        }
        return home;
    }
}
