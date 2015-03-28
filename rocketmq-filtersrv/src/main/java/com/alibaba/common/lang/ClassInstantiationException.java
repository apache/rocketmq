package com.alibaba.common.lang;

import com.alibaba.common.lang.exception.ChainedException;


/**
 * 代表实例化类时失败的异常。
 * 
 * @author Michael Zhou
 * @version $Id: ClassInstantiationException.java 1291 2005-03-04 03:23:30Z
 *          baobao $
 */
public class ClassInstantiationException extends ChainedException {
    private static final long serialVersionUID = 3258408422113555761L;


    /**
     * 构造一个空的异常.
     */
    public ClassInstantiationException() {
        super();
    }


    /**
     * 构造一个异常, 指明异常的详细信息.
     * 
     * @param message
     *            详细信息
     */
    public ClassInstantiationException(String message) {
        super(message);
    }


    /**
     * 构造一个异常, 指明引起这个异常的起因.
     * 
     * @param cause
     *            异常的起因
     */
    public ClassInstantiationException(Throwable cause) {
        super(cause);
    }


    /**
     * 构造一个异常, 指明引起这个异常的起因.
     * 
     * @param message
     *            详细信息
     * @param cause
     *            异常的起因
     */
    public ClassInstantiationException(String message, Throwable cause) {
        super(message, cause);
    }
}
