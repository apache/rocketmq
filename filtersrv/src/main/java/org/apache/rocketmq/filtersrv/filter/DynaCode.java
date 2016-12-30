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

package org.apache.rocketmq.filtersrv.filter;

import java.io.BufferedWriter;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.tools.JavaCompiler;
import javax.tools.ToolProvider;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.filter.FilterAPI;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DynaCode {
    private static final Logger LOGGER = LoggerFactory.getLogger(LoggerName.FILTERSRV_LOGGER_NAME);

    private static final String FILE_SP = System.getProperty("file.separator");

    private static final String LINE_SP = System.getProperty("line.separator");

    private String sourcePath = System.getProperty("user.home") + FILE_SP + "rocketmq_filter_class" + FILE_SP
        + UtilAll.getPid();

    private String outPutClassPath = sourcePath;

    private ClassLoader parentClassLoader;

    private List<String> codeStrs;

    private Map<String/* fullClassName */, Class<?>/* class */> loadClass;

    private String classpath;

    private String bootclasspath;

    private String extdirs;

    private String encoding = "UTF-8";

    private String target;

    @SuppressWarnings("unchecked")
    public DynaCode(String code) {
        this(Thread.currentThread().getContextClassLoader(), Arrays.asList(code));
    }

    public DynaCode(ClassLoader parentClassLoader, List<String> codeStrs) {
        this(extractClasspath(parentClassLoader), parentClassLoader, codeStrs);
    }

    public DynaCode(String classpath, ClassLoader parentClassLoader, List<String> codeStrs) {
        this.classpath = classpath;
        this.parentClassLoader = parentClassLoader;
        this.codeStrs = codeStrs;
        this.loadClass = new HashMap<String, Class<?>>(codeStrs.size());
    }

    public DynaCode(List<String> codeStrs) {
        this(Thread.currentThread().getContextClassLoader(), codeStrs);
    }

    private static String extractClasspath(ClassLoader cl) {
        StringBuffer buf = new StringBuffer();
        while (cl != null) {
            if (cl instanceof URLClassLoader) {
                URL urls[] = ((URLClassLoader) cl).getURLs();
                for (int i = 0; i < urls.length; i++) {
                    if (buf.length() > 0) {
                        buf.append(File.pathSeparatorChar);
                    }
                    String s = urls[i].getFile();
                    try {
                        s = URLDecoder.decode(s, "UTF-8");
                    } catch (UnsupportedEncodingException e) {
                        continue;
                    }
                    File f = new File(s);
                    buf.append(f.getAbsolutePath());
                }
            }
            cl = cl.getParent();
        }
        return buf.toString();
    }

    public static Class<?> compileAndLoadClass(final String className, final String javaSource)
        throws Exception {
        String classSimpleName = FilterAPI.simpleClassName(className);
        String javaCode = javaSource;

        final String newClassSimpleName = classSimpleName + System.currentTimeMillis();
        String newJavaCode = javaCode.replaceAll(classSimpleName, newClassSimpleName);

        List<String> codes = new ArrayList<String>();
        codes.add(newJavaCode);
        DynaCode dc = new DynaCode(codes);
        dc.compileAndLoadClass();
        Map<String, Class<?>> map = dc.getLoadClass();

        Class<?> clazz = map.get(getQualifiedName(newJavaCode));
        return clazz;
    }

    public static String getQualifiedName(String code) {
        StringBuilder sb = new StringBuilder();
        String className = getClassName(code);
        if (StringUtils.isNotBlank(className)) {

            String packageName = getPackageName(code);
            if (StringUtils.isNotBlank(packageName)) {
                sb.append(packageName).append(".");
            }
            sb.append(className);
        }
        return sb.toString();
    }

    public static String getClassName(String code) {
        String className = StringUtils.substringBefore(code, "{");
        if (StringUtils.isBlank(className)) {
            return className;
        }
        if (StringUtils.contains(code, " class ")) {
            className = StringUtils.substringAfter(className, " class ");
            if (StringUtils.contains(className, " extends ")) {
                className = StringUtils.substringBefore(className, " extends ").trim();
            } else if (StringUtils.contains(className, " implements ")) {
                className = StringUtils.trim(StringUtils.substringBefore(className, " implements "));
            } else {
                className = StringUtils.trim(className);
            }
        } else if (StringUtils.contains(code, " interface ")) {
            className = StringUtils.substringAfter(className, " interface ");
            if (StringUtils.contains(className, " extends ")) {
                className = StringUtils.substringBefore(className, " extends ").trim();
            } else {
                className = StringUtils.trim(className);
            }
        } else if (StringUtils.contains(code, " enum ")) {
            className = StringUtils.trim(StringUtils.substringAfter(className, " enum "));
        } else {
            return StringUtils.EMPTY;
        }
        return className;
    }

    public static String getPackageName(String code) {
        String packageName =
            StringUtils.substringBefore(StringUtils.substringAfter(code, "package "), ";").trim();
        return packageName;
    }

    public static String getFullClassName(String code) {
        String packageName = getPackageName(code);
        String className = getClassName(code);
        return StringUtils.isBlank(packageName) ? className : packageName + "." + className;
    }

    public void compileAndLoadClass() throws Exception {
        String[] sourceFiles = this.uploadSrcFile();
        this.compile(sourceFiles);
        this.loadClass(this.loadClass.keySet());
    }

    public Map<String, Class<?>> getLoadClass() {
        return loadClass;
    }

    private String[] uploadSrcFile() throws Exception {
        List<String> srcFileAbsolutePaths = new ArrayList<String>(codeStrs.size());
        for (String code : codeStrs) {
            if (StringUtils.isNotBlank(code)) {
                String packageName = getPackageName(code);
                String className = getClassName(code);
                if (StringUtils.isNotBlank(className)) {
                    File srcFile = null;
                    BufferedWriter bufferWriter = null;
                    try {
                        if (StringUtils.isBlank(packageName)) {
                            File pathFile = new File(sourcePath);

                            if (!pathFile.exists()) {
                                if (!pathFile.mkdirs()) {
                                    throw new RuntimeException("create PathFile Error!");
                                }
                            }
                            srcFile = new File(sourcePath + FILE_SP + className + ".java");
                        } else {
                            String srcPath = StringUtils.replace(packageName, ".", FILE_SP);
                            File pathFile = new File(sourcePath + FILE_SP + srcPath);

                            if (!pathFile.exists()) {
                                if (!pathFile.mkdirs()) {
                                    throw new RuntimeException("create PathFile Error!");
                                }
                            }
                            srcFile = new File(pathFile.getAbsolutePath() + FILE_SP + className + ".java");
                        }
                        synchronized (loadClass) {
                            loadClass.put(getFullClassName(code), null);
                        }
                        if (null != srcFile) {
                            LOGGER.warn("Dyna Create Java Source File:---->" + srcFile.getAbsolutePath());
                            srcFileAbsolutePaths.add(srcFile.getAbsolutePath());
                            srcFile.deleteOnExit();
                        }
                        OutputStreamWriter outputStreamWriter =
                            new OutputStreamWriter(new FileOutputStream(srcFile), encoding);
                        bufferWriter = new BufferedWriter(outputStreamWriter);
                        for (String lineCode : code.split(LINE_SP)) {
                            bufferWriter.write(lineCode);
                            bufferWriter.newLine();
                        }
                        bufferWriter.flush();
                    } finally {
                        if (null != bufferWriter) {
                            bufferWriter.close();
                        }
                    }
                }
            }
        }
        return srcFileAbsolutePaths.toArray(new String[srcFileAbsolutePaths.size()]);
    }

    private void compile(String[] srcFiles) throws Exception {
        String args[] = this.buildCompileJavacArgs(srcFiles);
        ByteArrayOutputStream err = new ByteArrayOutputStream();
        JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
        if (compiler == null) {
            throw new NullPointerException(
                "ToolProvider.getSystemJavaCompiler() return null,please use JDK replace JRE!");
        }
        int resultCode = compiler.run(null, null, err, args);
        if (resultCode != 0) {
            throw new Exception(err.toString(RemotingHelper.DEFAULT_CHARSET));
        }
    }

    private void loadClass(Set<String> classFullNames) throws ClassNotFoundException, MalformedURLException {
        synchronized (loadClass) {
            ClassLoader classLoader =
                new URLClassLoader(new URL[] {new File(outPutClassPath).toURI().toURL()},
                    parentClassLoader);
            for (String key : classFullNames) {
                Class<?> classz = classLoader.loadClass(key);
                if (null != classz) {
                    loadClass.put(key, classz);
                    LOGGER.info("Dyna Load Java Class File OK:----> className: " + key);
                } else {
                    LOGGER.error("Dyna Load Java Class File Fail:----> className: " + key);
                }
            }
        }
    }

    private String[] buildCompileJavacArgs(String srcFiles[]) {
        ArrayList<String> args = new ArrayList<String>();
        if (StringUtils.isNotBlank(classpath)) {
            args.add("-classpath");
            args.add(classpath);
        }
        if (StringUtils.isNotBlank(outPutClassPath)) {
            args.add("-d");
            args.add(outPutClassPath);
        }
        if (StringUtils.isNotBlank(sourcePath)) {
            args.add("-sourcepath");
            args.add(sourcePath);
        }
        if (StringUtils.isNotBlank(bootclasspath)) {
            args.add("-bootclasspath");
            args.add(bootclasspath);
        }
        if (StringUtils.isNotBlank(extdirs)) {
            args.add("-extdirs");
            args.add(extdirs);
        }
        if (StringUtils.isNotBlank(encoding)) {
            args.add("-encoding");
            args.add(encoding);
        }
        if (StringUtils.isNotBlank(target)) {
            args.add("-target");
            args.add(target);
        }
        for (int i = 0; i < srcFiles.length; i++) {
            args.add(srcFiles[i]);
        }
        return args.toArray(new String[args.size()]);
    }

    public String getOutPutClassPath() {
        return outPutClassPath;
    }

    public void setOutPutClassPath(String outPutClassPath) {
        this.outPutClassPath = outPutClassPath;
    }

    public String getSourcePath() {
        return sourcePath;
    }

    public void setSourcePath(String sourcePath) {
        this.sourcePath = sourcePath;
    }

    public ClassLoader getParentClassLoader() {
        return parentClassLoader;
    }

    public void setParentClassLoader(ClassLoader parentClassLoader) {
        this.parentClassLoader = parentClassLoader;
    }

    public String getClasspath() {
        return classpath;
    }

    public void setClasspath(String classpath) {
        this.classpath = classpath;
    }

    public String getBootclasspath() {
        return bootclasspath;
    }

    public void setBootclasspath(String bootclasspath) {
        this.bootclasspath = bootclasspath;
    }

    public String getExtdirs() {
        return extdirs;
    }

    public void setExtdirs(String extdirs) {
        this.extdirs = extdirs;
    }

    public String getEncoding() {
        return encoding;
    }

    public void setEncoding(String encoding) {
        this.encoding = encoding;
    }

    public String getTarget() {
        return target;
    }

    public void setTarget(String target) {
        this.target = target;
    }
}