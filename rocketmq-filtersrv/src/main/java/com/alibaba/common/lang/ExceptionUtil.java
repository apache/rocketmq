package com.alibaba.common.lang;

import java.io.PrintWriter;
import java.io.StringWriter;


/**
 * 处理异常的工具类。
 * 
 * @author Michael Zhou
 * @version $Id: ExceptionUtil.java 965 2004-04-28 03:20:05Z baobao $
 */
public class ExceptionUtil {
    /**
     * 取得异常的stacktrace字符串。
     * 
     * @param throwable
     *            异常
     * 
     * @return stacktrace字符串
     */
    public static String getStackTrace(Throwable throwable) {
        StringWriter buffer = new StringWriter();
        PrintWriter out = new PrintWriter(buffer);

        throwable.printStackTrace(out);
        out.flush();

        return buffer.toString();
    }
}
