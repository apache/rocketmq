/**
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

package com.alibaba.rocketmq.filtersrv.filter;

import com.alibaba.rocketmq.common.UtilAll;
import com.alibaba.rocketmq.common.constant.LoggerName;
import com.alibaba.rocketmq.common.filter.FilterAPI;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.tools.JavaCompiler;
import javax.tools.ToolProvider;
import java.io.*;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.net.URLDecoder;
import java.util.*;


public class DynaCode {
    private static final Logger logger = LoggerFactory.getLogger(LoggerName.FiltersrvLoggerName);

    private static final String FILE_SP = System.getProperty("file.separator");

    private static final String LINE_SP = System.getProperty("line.separator");

    /**
     * 生成java文件的路径
     */
    private String sourcePath = System.getProperty("user.home") + FILE_SP + "rocketmq_filter_class" + FILE_SP
            + UtilAll.getPid();

    /**
     * 生成class文件的路径
     */
    private String outPutClassPath = sourcePath;

    /**
     * 编译使用的生成ClassPath的ClassLoader
     */
    private ClassLoader parentClassLoader;

    /**
     * java的text代码
     */
    private List<String> codeStrs;

    /**
     * 已经加载好的class
     */
    private Map<String/* fullClassName */, Class<?>/* class */> loadClass;

    /**
     * 编译参数,使用的classpath，如果不指定则使用当前给定的 classloder所具有的classpath进行编译
     */
    private String classpath;

    /**
     * 编译参数，同javac的bootclasspath
     */
    private String bootclasspath;

    /**
     * 编译参数，同javac的extdirs
     */
    private String extdirs;

    /**
     * 编译参数，同javac的encoding
     */
    private String encoding = "UTF-8";

    /**
     * 编译参数，同javac的target
     */
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

    /**
     * 根据给定的classLoader获取其对应的classPath的完整字符串 路径 URLClassLoader.
     */
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


    public DynaCode(List<String> codeStrs) {
        this(Thread.currentThread().getContextClassLoader(), codeStrs);
    }

    public static Class<?> compileAndLoadClass(final String className, final String javaSource)
            throws Exception {
        String classSimpleName = FilterAPI.simpleClassName(className);
        String javaCode = new String(javaSource);
        // Java类名需要替换，否则可能会产生Source变更，但是无法加载的类冲突问题
        final String newClassSimpleName = classSimpleName + System.currentTimeMillis();
        String newJavaCode = javaCode.replaceAll(classSimpleName, newClassSimpleName);

        List<String> codes = new ArrayList<String>();
        codes.add(newJavaCode);
        // 创建DynaCode
        DynaCode dc = new DynaCode(codes);
        // 执行编译并且load
        dc.compileAndLoadClass();
        // 获取对应的clazz
        Map<String, Class<?>> map = dc.getLoadClass();
        // 反射执行结果
        Class<?> clazz = map.get(getQualifiedName(newJavaCode));
        return clazz;
    }

    /**
     * 编译并且加载给定的java编码类
     *
     * @throws Exception
     */
    public void compileAndLoadClass() throws Exception {
        String[] sourceFiles = this.uploadSrcFile();
        this.compile(sourceFiles);
        this.loadClass(this.loadClass.keySet());
    }

    public Map<String, Class<?>> getLoadClass() {
        return loadClass;
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

    /**
     * 将给定code的text生成java文件，并且写入硬盘
     *
     * @return
     *
     * @throws Exception
     */
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
                            // 如果不存在就创建
                            if (!pathFile.exists()) {
                                if (!pathFile.mkdirs()) {
                                    throw new RuntimeException("create PathFile Error!");
                                }
                            }
                            srcFile = new File(sourcePath + FILE_SP + className + ".java");
                        } else {
                            String srcPath = StringUtils.replace(packageName, ".", FILE_SP);
                            File pathFile = new File(sourcePath + FILE_SP + srcPath);
                            // 如果不存在就创建
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
                            logger.warn("Dyna Create Java Source File:---->" + srcFile.getAbsolutePath());
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

    /**
     * 编译给定文件绝对路径的java文件列表
     *
     * @param srcFiles
     *
     * @throws Exception
     */
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
            throw new Exception(err.toString());
        }
    }

    /**
     * 加载给定className的class
     *
     * @param classFullNames
     *
     * @throws ClassNotFoundException
     * @throws MalformedURLException
     */
    private void loadClass(Set<String> classFullNames) throws ClassNotFoundException, MalformedURLException {
        synchronized (loadClass) {
            // 使用outPutClassPath的URL创建个新的ClassLoader
            ClassLoader classLoader =
                    new URLClassLoader(new URL[]{new File(outPutClassPath).toURI().toURL()},
                            parentClassLoader);
            for (String key : classFullNames) {
                Class<?> classz = classLoader.loadClass(key);
                if (null != classz) {
                    loadClass.put(key, classz);
                    logger.info("Dyna Load Java Class File OK:----> className: " + key);
                } else {
                    logger.error("Dyna Load Java Class File Fail:----> className: " + key);
                }
            }
        }
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

    /**
     * 根据给定文件列表和当前的编译参数来构建 调用javac的编译参数数组
     *
     * @param srcFiles
     *
     * @return
     */
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