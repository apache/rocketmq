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

package com.alibaba.common.lang;

import com.alibaba.common.lang.exception.ChainedRuntimeException;


/**
 * 当<code>ObjectUtil.clone</code>方法被调用时，如果被复制的对象不支持该操作，则抛出该异常。
 * <p/>
 * <p>
 * 注意，和<code>java.lang.CloneNotSupportedException</code>不同，该异常从
 * <code>RuntimeException</code>派生。
 * </p>
 *
 * @author Michael Zhou
 * @version $Id: CloneNotSupportedException.java 1291 2005-03-04 03:23:30Z
 *          baobao $
 */
public class CloneNotSupportedException extends ChainedRuntimeException {
    private static final long serialVersionUID = 3257281439807584562L;


    /**
     * 构造一个空的异常.
     */
    public CloneNotSupportedException() {
        super();
    }


    /**
     * 构造一个异常, 指明异常的详细信息.
     *
     * @param message
     *         详细信息
     */
    public CloneNotSupportedException(String message) {
        super(message);
    }


    /**
     * 构造一个异常, 指明引起这个异常的起因.
     *
     * @param cause
     *         异常的起因
     */
    public CloneNotSupportedException(Throwable cause) {
        super(cause);
    }


    /**
     * 构造一个异常, 指明引起这个异常的起因.
     *
     * @param message
     *         详细信息
     * @param cause
     *         异常的起因
     */
    public CloneNotSupportedException(String message, Throwable cause) {
        super(message, cause);
    }
}
