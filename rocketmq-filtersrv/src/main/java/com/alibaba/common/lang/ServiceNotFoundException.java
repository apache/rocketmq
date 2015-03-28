package com.alibaba.common.lang;

import java.io.PrintStream;
import java.io.PrintWriter;

import com.alibaba.common.lang.exception.ChainedThrowable;
import com.alibaba.common.lang.exception.ChainedThrowableDelegate;


/**
 * 代表<code>META-INF/services/</code>中的文件未找到或读文件失败的异常。
 * 
 * @author Michael Zhou
 * @version $Id: ServiceNotFoundException.java 1291 2005-03-04 03:23:30Z baobao
 *          $
 */
public class ServiceNotFoundException extends ClassNotFoundException implements ChainedThrowable {
    private static final long serialVersionUID = 3258126964232566584L;
    private final ChainedThrowable delegate = new ChainedThrowableDelegate(this);
    private Throwable cause;


    /**
     * 构造一个空的异常.
     */
    public ServiceNotFoundException() {
        super();
    }


    /**
     * 构造一个异常, 指明异常的详细信息.
     * 
     * @param message
     *            详细信息
     */
    public ServiceNotFoundException(String message) {
        super(message);
    }


    /**
     * 构造一个异常, 指明引起这个异常的起因.
     * 
     * @param cause
     *            异常的起因
     */
    public ServiceNotFoundException(Throwable cause) {
        super((cause == null) ? null : cause.getMessage());
        this.cause = cause;
    }


    /**
     * 构造一个异常, 指明引起这个异常的起因.
     * 
     * @param message
     *            详细信息
     * @param cause
     *            异常的起因
     */
    public ServiceNotFoundException(String message, Throwable cause) {
        super(message);
        this.cause = cause;
    }


    /**
     * 取得引起这个异常的起因.
     * 
     * @return 异常的起因.
     */
    public Throwable getCause() {
        return cause;
    }


    /**
     * 打印调用栈到标准错误.
     */
    public void printStackTrace() {
        delegate.printStackTrace();
    }


    /**
     * 打印调用栈到指定输出流.
     * 
     * @param stream
     *            输出字节流.
     */
    public void printStackTrace(PrintStream stream) {
        delegate.printStackTrace(stream);
    }


    /**
     * 打印调用栈到指定输出流.
     * 
     * @param writer
     *            输出字符流.
     */
    public void printStackTrace(PrintWriter writer) {
        delegate.printStackTrace(writer);
    }


    /**
     * 打印异常的调用栈, 不包括起因异常的信息.
     * 
     * @param writer
     *            打印到输出流
     */
    public void printCurrentStackTrace(PrintWriter writer) {
        super.printStackTrace(writer);
    }
}
