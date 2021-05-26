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
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.rocketmq.test.util;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Properties;

public class FileUtil {
    private static String lineSeperator = System.getProperty("line.separator");

    private String filePath = "";
    private String fileName = "";

    public FileUtil(String filePath, String fileName) {
        this.filePath = filePath;
        this.fileName = fileName;
    }

    public static void main(String[] args) {
        String filePath = FileUtil.class.getResource("/").getPath();
        String fileName = "test.txt";
        FileUtil fileUtil = new FileUtil(filePath, fileName);
        Properties properties = new Properties();
        properties.put("xx", "yy");
        properties.put("yy", "xx");
        fileUtil.writeProperties(properties);
    }

    public void deleteFile() {
        File file = new File(filePath + File.separator + fileName);
        if (file.exists()) {
            file.delete();
        }
    }

    public void appendFile(String content) {
        File file = openFile();
        String newContent = lineSeperator + content;
        writeFile(file, newContent, true);
    }

    public void coverFile(String content) {
        File file = openFile();
        writeFile(file, content, false);
    }

    public void writeProperties(Properties properties) {
        String content = getPropertiesAsString(properties);
        this.coverFile(content);
    }

    private String getPropertiesAsString(Properties properties) {
        StringBuilder sb = new StringBuilder();
        for (Object key : properties.keySet()) {
            sb.append(key).append("=").append(properties.getProperty((String) key))
                .append(lineSeperator);
        }
        return sb.toString();
    }

    private void writeFile(File file, String content, boolean append) {
        FileWriter writer = null;
        try {
            writer = new FileWriter(file.getAbsoluteFile(), append);
            writer.write(content);
            writer.flush();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (writer != null) {
                try {
                    writer.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private File openFile() {
        File file = new File(filePath + File.separator + fileName);
        if (!file.exists()) {
            try {
                file.createNewFile();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return file;
    }
}
