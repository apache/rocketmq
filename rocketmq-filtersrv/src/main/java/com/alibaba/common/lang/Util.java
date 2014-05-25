package com.alibaba.common.lang;

import com.alibaba.common.lang.enumeration.EnumUtil;
import com.alibaba.common.lang.i18n.LocaleUtil;
import com.alibaba.common.lang.io.StreamUtil;

import java.util.Collections;
import java.util.Map;

/**
 * 集成常用的工具类。
 *
 * @author Michael Zhou
 * @version $Id: Util.java 1196 2004-11-24 01:03:13Z baobao $
 */
public class Util {
    private static final ArrayUtil        ARRAY_UTIL         = new ArrayUtil();
    private static final ClassLoaderUtil  CLASS_LOADER_UTIL  = new ClassLoaderUtil();
    private static final ClassUtil        CLASS_UTIL         = new ClassUtil();
    private static final EnumUtil         ENUM_UTIL          = new EnumUtil();
    private static final ExceptionUtil    EXCEPTION_UTIL     = new ExceptionUtil();
    private static final FileUtil         FILE_UTIL          = new FileUtil();
    private static final LocaleUtil       LOCALE_UTIL        = new LocaleUtil();
    private static final MathUtil         MATH_UTIL          = new MathUtil();
    private static final MessageUtil      MESSAGE_UTIL       = new MessageUtil();
    private static final ObjectUtil       OBJECT_UTIL        = new ObjectUtil();
    private static final StreamUtil       STREAM_UTIL        = new StreamUtil();
    private static final StringEscapeUtil STRING_ESCAPE_UTIL = new StringEscapeUtil();
    private static final StringUtil       STRING_UTIL        = new StringUtil();
    private static final SystemUtil       SYSTEM_UTIL        = new SystemUtil();
    private static final Map              ALL_UTILS          = Collections.unmodifiableMap(ArrayUtil
            .toMap(new Object[][] {
                    { "arrayUtil", ARRAY_UTIL },
                    { "classLoaderUtil", CLASS_LOADER_UTIL },
                    { "classUtil", CLASS_UTIL },
                    { "enumUtil", ENUM_UTIL },
                    { "exceptionUtil", EXCEPTION_UTIL },
                    { "fileUtil", FILE_UTIL },
                    { "localeUtil", LOCALE_UTIL },
                    { "mathUtil", MATH_UTIL },
                    { "messageUtil", MESSAGE_UTIL },
                    { "objectUtil", OBJECT_UTIL },
                    { "streamUtil", STREAM_UTIL },
                    { "stringEscapeUtil", STRING_ESCAPE_UTIL },
                    { "stringUtil", STRING_UTIL },
                    { "systemUtil", SYSTEM_UTIL }
                }));

    /**
     * 取得<code>ArrayUtil</code>。
     *
     * @return <code>ArrayUtil</code>实例
     */
    public static ArrayUtil getArrayUtil() {
        return ARRAY_UTIL;
    }

    /**
     * 取得<code>ClassLoaderUtil</code>。
     *
     * @return <code>ClassLoaderUtil</code>实例
     */
    public static ClassLoaderUtil getClassLoaderUtil() {
        return CLASS_LOADER_UTIL;
    }

    /**
     * 取得<code>ClassUtil</code>。
     *
     * @return <code>ClassUtil</code>实例
     */
    public static ClassUtil getClassUtil() {
        return CLASS_UTIL;
    }

    /**
     * 取得<code>EnumUtil</code>。
     *
     * @return <code>EnumUtil</code>实例
     */
    public static EnumUtil getEnumUtil() {
        return ENUM_UTIL;
    }

    /**
     * 取得<code>ExceptionUtil</code>。
     *
     * @return <code>ExceptionUtil</code>实例
     */
    public static ExceptionUtil getExceptionUtil() {
        return EXCEPTION_UTIL;
    }

    /**
     * 取得<code>FileUtil</code>。
     *
     * @return <code>FileUtil</code>实例
     */
    public static FileUtil getFileUtil() {
        return FILE_UTIL;
    }

    /**
     * 取得<code>LocaleUtil</code>。
     *
     * @return <code>LocaleUtil</code>实例
     */
    public static LocaleUtil getLocaleUtil() {
        return LOCALE_UTIL;
    }

    /**
     * 取得<code>MathUtil</code>。
     *
     * @return <code>MathUtil</code>实例
     */
    public static MathUtil getMathUtil() {
        return MATH_UTIL;
    }

    /**
     * 取得<code>MessageUtil</code>。
     *
     * @return <code>MessageUtil</code>实例
     */
    public static MessageUtil getMessageUtil() {
        return MESSAGE_UTIL;
    }

    /**
     * 取得<code>ObjectUtil</code>。
     *
     * @return <code>ObjectUtil</code>实例
     */
    public static ObjectUtil getObjectUtil() {
        return OBJECT_UTIL;
    }

    /**
     * 取得<code>StreamUtil</code>。
     *
     * @return <code>StreamUtil</code>实例
     */
    public static StreamUtil getStreamUtil() {
        return STREAM_UTIL;
    }

    /**
     * 取得<code>StringEscapeUtil</code>。
     *
     * @return <code>StringEscapeUtil</code>实例
     */
    public static StringEscapeUtil getStringEscapeUtil() {
        return STRING_ESCAPE_UTIL;
    }

    /**
     * 取得<code>StringUtil</code>。
     *
     * @return <code>StringUtil</code>实例
     */
    public static StringUtil getStringUtil() {
        return STRING_UTIL;
    }

    /**
     * 取得<code>SystemUtil</code>。
     *
     * @return <code>SystemUtil</code>实例
     */
    public static SystemUtil getSystemUtil() {
        return SYSTEM_UTIL;
    }

    /**
     * 取得包含所有utils的map
     *
     * @return utils map
     */
    public static Map getUtils() {
        return ALL_UTILS;
    }
}
