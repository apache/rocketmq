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

package com.alibaba.common.lang.exception;

import java.io.PrintStream;
import java.io.PrintWriter;
import java.io.Serializable;


/**
 * 实现此接口的异常, 是由另一个异常引起的.
 *
 * @author Michael Zhou
 * @version $Id: ChainedThrowable.java 509 2004-02-16 05:42:07Z baobao $
 */
public interface ChainedThrowable extends Serializable {
    /**
     * 取得异常的起因.
     *
     * @return 异常的起因.
     */
    Throwable getCause();


    /**
     * 打印调用栈到标准错误.
     */
    void printStackTrace();


    /**
     * 打印调用栈到指定输出流.
     *
     * @param stream
     *         输出字节流.
     */
    void printStackTrace(PrintStream stream);


    /**
     * 打印调用栈到指定输出流.
     *
     * @param writer
     *         输出字符流.
     */
    void printStackTrace(PrintWriter writer);


    /**
     * 打印异常的调用栈, 不包括起因异常的信息.
     *
     * @param writer
     *         打印到输出流
     */
    void printCurrentStackTrace(PrintWriter writer);
}
