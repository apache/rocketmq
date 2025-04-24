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

package org.apache.rocketmq.auth.migration.plain;

import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import org.junit.Assert;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.JarURLConnection;
import java.net.URL;
import java.nio.file.Files;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.UUID;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

public final class AclTestHelper {
    private AclTestHelper() {
    }

    private static void copyTo(String path, InputStream src, File dstDir, String flag, boolean into)
        throws IOException {
        Preconditions.checkNotNull(flag);
        Iterator<String> iterator = Splitter.on(File.separatorChar).split(path).iterator();
        boolean found = false;
        File dir = dstDir;
        while (iterator.hasNext()) {
            String current = iterator.next();
            if (!found && flag.equals(current)) {
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
                if (!iterator.hasNext()) {
                    dir = new File(dir, current);
                } else {
                    dir = new File(dir, current);
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
            file.delete();
        } else {
            File[] files = file.listFiles();
            if (null != files) {
                for (File f : files) {
                    recursiveDelete(f);
                }
            }
            file.delete();
        }
    }

    public static File copyResources(String folder) throws IOException {
        return copyResources(folder, false);
    }

    public static File copyResources(String folder, boolean into) throws IOException {
        File home = new File(System.getProperty("java.io.tmpdir"), UUID.randomUUID().toString().replace('-', '_'));
        if (!home.exists()) {
            Assert.assertTrue(home.mkdirs());
        }
        
        // Get the resource URL for the folder
        URL folderUrl = AclTestHelper.class.getClassLoader().getResource(folder);
        if (folderUrl == null) {
            throw new IOException("Resource folder not found: " + folder);
        }
        
        // Check if the resource is in a JAR or in the file system
        if ("file".equals(folderUrl.getProtocol())) {
            // Resource is in the file system
            File sourceDir = new File(folderUrl.getFile());
            copyDirectory(sourceDir, home, folder, into);
        } else if ("jar".equals(folderUrl.getProtocol())) {
            // Resource is in a JAR file
            copyResourcesFromJar(folderUrl, home, folder, into);
        } else {
            throw new IOException("Unsupported protocol: " + folderUrl.getProtocol());
        }
        
        return home;
    }
    
    private static void copyDirectory(File sourceDir, File destDir, String folderName, boolean into) throws IOException {
        if (!sourceDir.isDirectory()) {
            return;
        }
        
        File[] files = sourceDir.listFiles();
        if (files == null) {
            return;
        }
        
        for (File file : files) {
            String path = file.getAbsolutePath();
            try (InputStream inputStream = Files.newInputStream(file.toPath())) {
                copyTo(path, inputStream, destDir, folderName, into);
            }
        }
    }
    
    private static void copyResourcesFromJar(URL jarUrl, File destDir, String folderName, boolean into) throws IOException {
        try {
            JarURLConnection jarConnection = (JarURLConnection) jarUrl.openConnection();
            JarFile jar = jarConnection.getJarFile();
            
            Enumeration<JarEntry> entries = jar.entries();
            String folderPath = folderName + "/";
            
            while (entries.hasMoreElements()) {
                JarEntry entry = entries.nextElement();
                String entryName = entry.getName();
                
                if (entryName.startsWith(folderPath) && !entry.isDirectory()) {
                    // Sanitize the entry name to prevent Zip Slip
                    File destFile = new File(destDir, entryName);
                    String destDirPath = destDir.getCanonicalPath();
                    String destFilePath = destFile.getCanonicalPath();
                    
                    if (!destFilePath.startsWith(destDirPath + File.separator)) {
                        throw new IOException("Entry is outside of the target dir: " + entryName);
                    }
                    
                    try (InputStream inputStream = jar.getInputStream(entry)) {
                        copyTo(entryName, inputStream, destDir, folderName, into);
                    }
                }
            }
        } catch (IOException e) {
            throw new IOException("Failed to copy resources from JAR", e);
        }
    }
}
