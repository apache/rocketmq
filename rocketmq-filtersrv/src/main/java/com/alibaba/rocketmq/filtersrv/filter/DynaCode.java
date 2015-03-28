package com.alibaba.rocketmq.filtersrv.filter;

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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.tools.JavaCompiler;
import javax.tools.ToolProvider;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.common.lang.ArrayUtil;
import com.alibaba.common.lang.StringUtil;
import com.alibaba.rocketmq.common.UtilAll;
import com.alibaba.rocketmq.common.constant.LoggerName;
import com.alibaba.rocketmq.common.filter.FilterAPI;


/**
 * <p>
 * description: java动态编译类，创建该类时需要给定编译的java代码的text的code字符串
 * 再调用compileAndLoadClass方法进行编译，调用compileAndLoadClass之前可以通过相关的 SET方法来设置编译的一些参数
 * <p>
 * 
 * @{# DynaCode.java Create on Sep 22, 2011 11:34:10 AM
 * 
 *     Copyright (c) 2011 by qihao.
 * 
 * @author <a href="mailto:qihao@taobao.com">qihao</a>
 * @version 1.0
 */
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
        this(Thread.currentThread().getContextClassLoader(), ArrayUtil.toList(new String[] { code }));
    }


    public DynaCode(List<String> codeStrs) {
        this(Thread.currentThread().getContextClassLoader(), codeStrs);
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
     * 编译并且加载给定的java编码类
     * 
     * @throws Exception
     */
    public void compileAndLoadClass() throws Exception {
        String[] sourceFiles = this.uploadSrcFile();
        this.compile(sourceFiles);
        this.loadClass(this.loadClass.keySet());
    }


    public static String getClassName(String code) {
        String className = StringUtil.substringBefore(code, "{");
        if (StringUtil.isBlank(className)) {
            return className;
        }
        if (StringUtil.contains(code, " class ")) {
            className = StringUtil.substringAfter(className, " class ");
            if (StringUtil.contains(className, " extends ")) {
                className = StringUtil.substringBefore(className, " extends ").trim();
            }
            else if (StringUtil.contains(className, " implements ")) {
                className = StringUtil.trim(StringUtil.substringBefore(className, " implements "));
            }
            else {
                className = StringUtil.trim(className);
            }
        }
        else if (StringUtil.contains(code, " interface ")) {
            className = StringUtil.substringAfter(className, " interface ");
            if (StringUtil.contains(className, " extends ")) {
                className = StringUtil.substringBefore(className, " extends ").trim();
            }
            else {
                className = StringUtil.trim(className);
            }
        }
        else if (StringUtil.contains(code, " enum ")) {
            className = StringUtil.trim(StringUtil.substringAfter(className, " enum "));
        }
        else {
            return StringUtil.EMPTY_STRING;
        }
        return className;
    }


    public static String getPackageName(String code) {
        String packageName =
                StringUtil.substringBefore(StringUtil.substringAfter(code, "package "), ";").trim();
        return packageName;
    }


    public static String getQualifiedName(String code) {
        StringBuilder sb = new StringBuilder();
        String className = getClassName(code);
        if (StringUtil.isNotBlank(className)) {

            String packageName = getPackageName(code);
            if (StringUtil.isNotBlank(packageName)) {
                sb.append(packageName).append(".");
            }
            sb.append(className);
        }
        return sb.toString();
    }


    public static String getFullClassName(String code) {
        String packageName = getPackageName(code);
        String className = getClassName(code);
        return StringUtil.isBlank(packageName) ? className : packageName + "." + className;
    }


    /**
     * 加载给定className的class
     * 
     * @param classFullNames
     * @throws ClassNotFoundException
     * @throws MalformedURLException
     */
    private void loadClass(Set<String> classFullNames) throws ClassNotFoundException, MalformedURLException {
        synchronized (loadClass) {
            // 使用outPutClassPath的URL创建个新的ClassLoader
            ClassLoader classLoader =
                    new URLClassLoader(new URL[] { new File(outPutClassPath).toURI().toURL() },
                        parentClassLoader);
            for (String key : classFullNames) {
                Class<?> classz = classLoader.loadClass(key);
                if (null != classz) {
                    loadClass.put(key, classz);
                    logger.info("Dyna Load Java Class File OK:----> className: " + key);
                }
                else {
                    logger.error("Dyna Load Java Class File Fail:----> className: " + key);
                }
            }
        }
    }


    /**
     * 编译给定文件绝对路径的java文件列表
     * 
     * @param srcFiles
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
     * 将给定code的text生成java文件，并且写入硬盘
     * 
     * @return
     * @throws Exception
     */
    private String[] uploadSrcFile() throws Exception {
        List<String> srcFileAbsolutePaths = new ArrayList<String>(codeStrs.size());
        for (String code : codeStrs) {
            if (StringUtil.isNotBlank(code)) {
                String packageName = getPackageName(code);
                String className = getClassName(code);
                if (StringUtil.isNotBlank(className)) {
                    File srcFile = null;
                    BufferedWriter bufferWriter = null;
                    try {
                        if (StringUtil.isBlank(packageName)) {
                            File pathFile = new File(sourcePath);
                            // 如果不存在就创建
                            if (!pathFile.exists()) {
                                if (!pathFile.mkdirs()) {
                                    throw new RuntimeException("create PathFile Error!");
                                }
                            }
                            srcFile = new File(sourcePath + FILE_SP + className + ".java");
                        }
                        else {
                            String srcPath = StringUtil.replace(packageName, ".", FILE_SP);
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
                    }
                    finally {
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
     * 根据给定文件列表和当前的编译参数来构建 调用javac的编译参数数组
     * 
     * @param srcFiles
     * @return
     */
    private String[] buildCompileJavacArgs(String srcFiles[]) {
        ArrayList<String> args = new ArrayList<String>();
        if (StringUtil.isNotBlank(classpath)) {
            args.add("-classpath");
            args.add(classpath);
        }
        if (StringUtil.isNotBlank(outPutClassPath)) {
            args.add("-d");
            args.add(outPutClassPath);
        }
        if (StringUtil.isNotBlank(sourcePath)) {
            args.add("-sourcepath");
            args.add(sourcePath);
        }
        if (StringUtil.isNotBlank(bootclasspath)) {
            args.add("-bootclasspath");
            args.add(bootclasspath);
        }
        if (StringUtil.isNotBlank(extdirs)) {
            args.add("-extdirs");
            args.add(extdirs);
        }
        if (StringUtil.isNotBlank(encoding)) {
            args.add("-encoding");
            args.add(encoding);
        }
        if (StringUtil.isNotBlank(target)) {
            args.add("-target");
            args.add(target);
        }
        for (int i = 0; i < srcFiles.length; i++) {
            args.add(srcFiles[i]);
        }
        return args.toArray(new String[args.size()]);
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
                    }
                    catch (UnsupportedEncodingException e) {
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


    public Map<String, Class<?>> getLoadClass() {
        return loadClass;
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
}