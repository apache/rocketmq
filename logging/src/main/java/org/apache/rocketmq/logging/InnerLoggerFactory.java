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

package org.apache.rocketmq.logging;

import org.apache.rocketmq.logging.inner.Logger;

import java.util.HashMap;
import java.util.Map;

public class InnerLoggerFactory extends InternalLoggerFactory {

    @Override
    protected InternalLogger doGetLogger(String name) {
        return new InnerLogger(name);
    }

    @Override
    protected String getLoggerType() {
        return LOGGER_INNER;
    }

    public static class InnerLogger implements InternalLogger {

        private Logger logger;

        public InnerLogger(String name) {
            logger = Logger.getLogger(name);
        }

        @Override
        public String getName() {
            return logger.getName();
        }

        @Override
        public boolean isTraceEnabled() {
            return logger.isTraceEnabled();
        }

        @Override
        public void trace(String var1) {
            logger.trace(var1);
        }

        @Override
        public void trace(String var1, Throwable var2) {
            logger.trace(var1, var2);
        }

        @Override
        public boolean isDebugEnabled() {
            return logger.isDebugEnabled();
        }

        @Override
        public void debug(String var1) {
            logger.debug(var1);
        }

        @Override
        public void debug(String var1, Throwable var2) {
            logger.debug(var1, var2);
        }

        @Override
        public boolean isInfoEnabled() {
            return logger.isInfoEnabled();
        }

        @Override
        public void info(String var1) {
            logger.info(var1);
        }

        @Override
        public void info(String var1, Throwable var2) {
            logger.info(var1, var2);
        }

        @Override
        public boolean isWarnEnabled() {
            return logger.isWarnEnabled();
        }

        @Override
        public void warn(String var1) {
            logger.warn(var1);
        }

        @Override
        public void warn(String var1, Throwable var2) {
            logger.warn(var1, var2);
        }

        @Override
        public boolean isErrorEnabled() {
            return logger.isErrorEnabled();
        }

        @Override
        public void error(String var1) {
            logger.error(var1);
        }

        @Override
        public void error(String var1, Throwable var2) {
            logger.error(var1, var2);
        }

        @Override
        public void trace(String var1, Object var2) {
            FormattingTuple format = MessageFormatter.format(var1, var2);
            logger.trace(format.getMessage(), format.getThrowable());
        }

        @Override
        public void trace(String var1, Object var2, Object var3) {
            FormattingTuple format = MessageFormatter.format(var1, var2, var3);
            logger.trace(format.getMessage(), format.getThrowable());
        }

        @Override
        public void trace(String var1, Object... var2) {
            FormattingTuple format = MessageFormatter.arrayFormat(var1, var2);
            logger.trace(format.getMessage(), format.getThrowable());
        }

        @Override
        public void debug(String var1, Object var2) {
            FormattingTuple format = MessageFormatter.format(var1, var2);
            logger.debug(format.getMessage(), format.getThrowable());
        }

        @Override
        public void debug(String var1, Object var2, Object var3) {
            FormattingTuple format = MessageFormatter.format(var1, var2, var3);
            logger.debug(format.getMessage(), format.getThrowable());
        }

        @Override
        public void debug(String var1, Object... var2) {
            FormattingTuple format = MessageFormatter.arrayFormat(var1, var2);
            logger.debug(format.getMessage(), format.getThrowable());
        }

        @Override
        public void info(String var1, Object var2) {
            FormattingTuple format = MessageFormatter.format(var1, var2);
            logger.info(format.getMessage(), format.getThrowable());
        }

        @Override
        public void info(String var1, Object var2, Object var3) {
            FormattingTuple format = MessageFormatter.format(var1, var2, var3);
            logger.info(format.getMessage(), format.getThrowable());
        }

        @Override
        public void info(String var1, Object... var2) {
            FormattingTuple format = MessageFormatter.arrayFormat(var1, var2);
            logger.info(format.getMessage(), format.getThrowable());
        }

        @Override
        public void warn(String var1, Object var2) {
            FormattingTuple format = MessageFormatter.format(var1, var2);
            logger.warn(format.getMessage(), format.getThrowable());
        }

        @Override
        public void warn(String var1, Object... var2) {
            FormattingTuple format = MessageFormatter.arrayFormat(var1, var2);
            logger.warn(format.getMessage(), format.getThrowable());
        }

        @Override
        public void warn(String var1, Object var2, Object var3) {
            FormattingTuple format = MessageFormatter.format(var1, var2, var3);
            logger.warn(format.getMessage(), format.getThrowable());
        }

        @Override
        public void error(String var1, Object var2) {
            FormattingTuple format = MessageFormatter.format(var1, var2);
            logger.warn(format.getMessage(), format.getThrowable());
        }

        @Override
        public void error(String var1, Object var2, Object var3) {
            FormattingTuple format = MessageFormatter.format(var1, var2, var3);
            logger.warn(format.getMessage(), format.getThrowable());
        }

        @Override
        public void error(String var1, Object... var2) {
            FormattingTuple format = MessageFormatter.arrayFormat(var1, var2);
            logger.warn(format.getMessage(), format.getThrowable());
        }

        public Logger getLogger() {
            return logger;
        }
    }


    private static class FormattingTuple {
        public static FormattingTuple NULL = new FormattingTuple((String) null);
        private String message;
        private Throwable throwable;
        private Object[] argArray;

        public FormattingTuple(String message) {
            this(message, (Object[]) null, (Throwable) null);
        }

        public FormattingTuple(String message, Object[] argArray, Throwable throwable) {
            this.message = message;
            this.throwable = throwable;
            if (throwable == null) {
                this.argArray = argArray;
            } else {
                this.argArray = trimmedCopy(argArray);
            }

        }

        static Object[] trimmedCopy(Object[] argArray) {
            if (argArray != null && argArray.length != 0) {
                int trimemdLen = argArray.length - 1;
                Object[] trimmed = new Object[trimemdLen];
                System.arraycopy(argArray, 0, trimmed, 0, trimemdLen);
                return trimmed;
            } else {
                throw new IllegalStateException("non-sensical empty or null argument array");
            }
        }

        public String getMessage() {
            return this.message;
        }

        public Object[] getArgArray() {
            return this.argArray;
        }

        public Throwable getThrowable() {
            return this.throwable;
        }
    }

    private static class MessageFormatter {
        static final char DELIM_START = '{';
        static final char DELIM_STOP = '}';
        static final String DELIM_STR = "{}";
        private static final char ESCAPE_CHAR = '\\';

        public MessageFormatter() {
        }

        public static final FormattingTuple format(String messagePattern, Object arg) {
            return arrayFormat(messagePattern, new Object[]{arg});
        }

        public static final FormattingTuple format(String messagePattern, Object arg1, Object arg2) {
            return arrayFormat(messagePattern, new Object[]{arg1, arg2});
        }

        static final Throwable getThrowableCandidate(Object[] argArray) {
            if (argArray != null && argArray.length != 0) {
                Object lastEntry = argArray[argArray.length - 1];
                return lastEntry instanceof Throwable ? (Throwable) lastEntry : null;
            } else {
                return null;
            }
        }

        public static final FormattingTuple arrayFormat(String messagePattern, Object[] argArray) {
            Throwable throwableCandidate = getThrowableCandidate(argArray);
            if (messagePattern == null) {
                return new FormattingTuple((String) null, argArray, throwableCandidate);
            } else if (argArray == null) {
                return new FormattingTuple(messagePattern);
            } else {
                int i = 0;
                StringBuilder sbuf = new StringBuilder(messagePattern.length() + 50);

                int L;
                for (L = 0; L < argArray.length; ++L) {
                    int j = messagePattern.indexOf("{}", i);
                    if (j == -1) {
                        if (i == 0) {
                            return new FormattingTuple(messagePattern, argArray, throwableCandidate);
                        }

                        sbuf.append(messagePattern.substring(i, messagePattern.length()));
                        return new FormattingTuple(sbuf.toString(), argArray, throwableCandidate);
                    }

                    if (isEscapedDelimeter(messagePattern, j)) {
                        if (!isDoubleEscaped(messagePattern, j)) {
                            --L;
                            sbuf.append(messagePattern.substring(i, j - 1));
                            sbuf.append('{');
                            i = j + 1;
                        } else {
                            sbuf.append(messagePattern.substring(i, j - 1));
                            deeplyAppendParameter(sbuf, argArray[L], new HashMap());
                            i = j + 2;
                        }
                    } else {
                        sbuf.append(messagePattern.substring(i, j));
                        deeplyAppendParameter(sbuf, argArray[L], new HashMap());
                        i = j + 2;
                    }
                }

                sbuf.append(messagePattern.substring(i, messagePattern.length()));
                if (L < argArray.length - 1) {
                    return new FormattingTuple(sbuf.toString(), argArray, throwableCandidate);
                } else {
                    return new FormattingTuple(sbuf.toString(), argArray, (Throwable) null);
                }
            }
        }

        static final boolean isEscapedDelimeter(String messagePattern, int delimeterStartIndex) {
            if (delimeterStartIndex == 0) {
                return false;
            } else {
                char potentialEscape = messagePattern.charAt(delimeterStartIndex - 1);
                return potentialEscape == 92;
            }
        }

        static final boolean isDoubleEscaped(String messagePattern, int delimeterStartIndex) {
            return delimeterStartIndex >= 2 && messagePattern.charAt(delimeterStartIndex - 2) == 92;
        }

        private static void deeplyAppendParameter(StringBuilder sbuf, Object o, Map<Object[], Object> seenMap) {
            if (o == null) {
                sbuf.append("null");
            } else {
                if (!o.getClass().isArray()) {
                    safeObjectAppend(sbuf, o);
                } else if (o instanceof boolean[]) {
                    booleanArrayAppend(sbuf, (boolean[]) ((boolean[]) o));
                } else if (o instanceof byte[]) {
                    byteArrayAppend(sbuf, (byte[]) ((byte[]) o));
                } else if (o instanceof char[]) {
                    charArrayAppend(sbuf, (char[]) ((char[]) o));
                } else if (o instanceof short[]) {
                    shortArrayAppend(sbuf, (short[]) ((short[]) o));
                } else if (o instanceof int[]) {
                    intArrayAppend(sbuf, (int[]) ((int[]) o));
                } else if (o instanceof long[]) {
                    longArrayAppend(sbuf, (long[]) ((long[]) o));
                } else if (o instanceof float[]) {
                    floatArrayAppend(sbuf, (float[]) ((float[]) o));
                } else if (o instanceof double[]) {
                    doubleArrayAppend(sbuf, (double[]) ((double[]) o));
                } else {
                    objectArrayAppend(sbuf, (Object[]) ((Object[]) o), seenMap);
                }

            }
        }

        private static void safeObjectAppend(StringBuilder sbuf, Object o) {
            try {
                String t = o.toString();
                sbuf.append(t);
            } catch (Throwable var3) {
                System.err.println("SLF4J: Failed toString() invocation on an object of type [" + o.getClass().getName() + "]");
                var3.printStackTrace();
                sbuf.append("[FAILED toString()]");
            }

        }

        private static void objectArrayAppend(StringBuilder sbuf, Object[] a, Map<Object[], Object> seenMap) {
            sbuf.append('[');
            if (!seenMap.containsKey(a)) {
                seenMap.put(a, (Object) null);
                int len = a.length;

                for (int i = 0; i < len; ++i) {
                    deeplyAppendParameter(sbuf, a[i], seenMap);
                    if (i != len - 1) {
                        sbuf.append(", ");
                    }
                }

                seenMap.remove(a);
            } else {
                sbuf.append("...");
            }

            sbuf.append(']');
        }

        private static void booleanArrayAppend(StringBuilder sbuf, boolean[] a) {
            sbuf.append('[');
            int len = a.length;

            for (int i = 0; i < len; ++i) {
                sbuf.append(a[i]);
                if (i != len - 1) {
                    sbuf.append(", ");
                }
            }

            sbuf.append(']');
        }

        private static void byteArrayAppend(StringBuilder sbuf, byte[] a) {
            sbuf.append('[');
            int len = a.length;

            for (int i = 0; i < len; ++i) {
                sbuf.append(a[i]);
                if (i != len - 1) {
                    sbuf.append(", ");
                }
            }

            sbuf.append(']');
        }

        private static void charArrayAppend(StringBuilder sbuf, char[] a) {
            sbuf.append('[');
            int len = a.length;

            for (int i = 0; i < len; ++i) {
                sbuf.append(a[i]);
                if (i != len - 1) {
                    sbuf.append(", ");
                }
            }

            sbuf.append(']');
        }

        private static void shortArrayAppend(StringBuilder sbuf, short[] a) {
            sbuf.append('[');
            int len = a.length;

            for (int i = 0; i < len; ++i) {
                sbuf.append(a[i]);
                if (i != len - 1) {
                    sbuf.append(", ");
                }
            }

            sbuf.append(']');
        }

        private static void intArrayAppend(StringBuilder sbuf, int[] a) {
            sbuf.append('[');
            int len = a.length;

            for (int i = 0; i < len; ++i) {
                sbuf.append(a[i]);
                if (i != len - 1) {
                    sbuf.append(", ");
                }
            }

            sbuf.append(']');
        }

        private static void longArrayAppend(StringBuilder sbuf, long[] a) {
            sbuf.append('[');
            int len = a.length;

            for (int i = 0; i < len; ++i) {
                sbuf.append(a[i]);
                if (i != len - 1) {
                    sbuf.append(", ");
                }
            }

            sbuf.append(']');
        }

        private static void floatArrayAppend(StringBuilder sbuf, float[] a) {
            sbuf.append('[');
            int len = a.length;

            for (int i = 0; i < len; ++i) {
                sbuf.append(a[i]);
                if (i != len - 1) {
                    sbuf.append(", ");
                }
            }

            sbuf.append(']');
        }

        private static void doubleArrayAppend(StringBuilder sbuf, double[] a) {
            sbuf.append('[');
            int len = a.length;

            for (int i = 0; i < len; ++i) {
                sbuf.append(a[i]);
                if (i != len - 1) {
                    sbuf.append(", ");
                }
            }

            sbuf.append(']');
        }
    }
}
