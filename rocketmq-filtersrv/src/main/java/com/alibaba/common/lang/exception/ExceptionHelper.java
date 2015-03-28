package com.alibaba.common.lang.exception;

import java.io.PrintStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;


/**
 * 异常的辅助类.
 * 
 * @author Michael Zhou
 * @version $Id: ExceptionHelper.java 643 2004-03-07 07:29:35Z baobao $
 */
public class ExceptionHelper {
    private static final String STRING_EXCEPTION_MESSAGE = ": ";
    private static final String STRING_CAUSED_BY = "Caused by: ";
    private static final String STRING_MORE_PREFIX = "\t... ";
    private static final String STRING_MORE_SUFFIX = " more";
    private static final String STRING_STACK_TRACE_PREFIX = "\tat ";
    private static final String STRING_CR = "\r";
    private static final String STRING_LF = "\n";
    private static final String GET_STACK_TRACE_METHOD_NAME = "getStackTrace";
    private static Method GET_STACK_TRACE_METHOD;

    static {
        // JDK1.4支持Throwable.getStackTrace()方法
        try {
            GET_STACK_TRACE_METHOD = Throwable.class.getMethod(GET_STACK_TRACE_METHOD_NAME, new Class[0]);
        }
        catch (NoSuchMethodException e) {
        }
    }


    /**
     * 从<code>ChainedThrowable</code>实例中取得<code>Throwable</code>对象.
     * 
     * @param throwable
     *            <code>ChainedThrowable</code>实例
     * 
     * @return <code>Throwable</code>对象
     */
    private static Throwable getThrowable(ChainedThrowable throwable) {
        if (throwable instanceof ChainedThrowableDelegate) {
            return ((ChainedThrowableDelegate) throwable).delegatedThrowable;
        }
        else {
            return (Throwable) throwable;
        }
    }


    /**
     * 将<code>Throwable</code>转换成<code>ChainedThrowable</code>. 如果已经是
     * <code>ChainedThrowable</code>了, 则直接返回, 否则将它包装在
     * <code>ChainedThrowableDelegate</code>中返回.
     * 
     * @param throwable
     *            <code>Throwable</code>对象
     * 
     * @return <code>ChainedThrowable</code>对象
     */
    public static ChainedThrowable getChainedThrowable(Throwable throwable) {
        if ((throwable != null) && !(throwable instanceof ChainedThrowable)) {
            return new ChainedThrowableDelegate(throwable);
        }

        return (ChainedThrowable) throwable;
    }


    /**
     * 取得被代理的异常的起因, 如果起因不是<code>ChainedThrowable</code>, 则用
     * <code>ChainedThrowableDelegate</code>包装并返回.
     * 
     * @param throwable
     *            异常
     * 
     * @return 异常的起因
     */
    public static ChainedThrowable getChainedThrowableCause(ChainedThrowable throwable) {
        return getChainedThrowable(throwable.getCause());
    }


    /**
     * 打印调用栈到标准错误.
     * 
     * @param throwable
     *            异常
     */
    public static void printStackTrace(ChainedThrowable throwable) {
        printStackTrace(throwable, System.err);
    }


    /**
     * 打印调用栈到指定输出流.
     * 
     * @param throwable
     *            异常
     * @param stream
     *            输出字节流
     */
    public static void printStackTrace(ChainedThrowable throwable, PrintStream stream) {
        printStackTrace(throwable, new PrintWriter(stream));
    }


    /**
     * 打印调用栈到指定输出流.
     * 
     * @param throwable
     *            异常
     * @param writer
     *            输出字符流
     */
    public static void printStackTrace(ChainedThrowable throwable, PrintWriter writer) {
        synchronized (writer) {
            String[] currentStack = analyzeStackTrace(throwable);

            printThrowableMessage(throwable, writer, false);

            for (int i = 0; i < currentStack.length; i++) {
                writer.println(currentStack[i]);
            }

            printStackTraceRecursive(throwable, writer, currentStack);

            writer.flush();
        }
    }


    /**
     * 递归地打印所有异常链的调用栈.
     * 
     * @param throwable
     *            异常
     * @param writer
     *            输出流
     * @param currentStack
     *            当前的堆栈
     */
    private static void printStackTraceRecursive(ChainedThrowable throwable, PrintWriter writer,
            String[] currentStack) {
        ChainedThrowable cause = getChainedThrowableCause(throwable);

        if (cause != null) {
            String[] causeStack = analyzeStackTrace(cause);
            int i = currentStack.length - 1;
            int j = causeStack.length - 1;

            for (; (i >= 0) && (j >= 0); i--, j--) {
                if (!currentStack[i].equals(causeStack[j])) {
                    break;
                }
            }

            printThrowableMessage(cause, writer, true);

            for (i = 0; i <= j; i++) {
                writer.println(causeStack[i]);
            }

            if (j < (causeStack.length - 1)) {
                writer.println(STRING_MORE_PREFIX + (causeStack.length - j - 1) + STRING_MORE_SUFFIX);
            }

            printStackTraceRecursive(cause, writer, causeStack);
        }
    }


    /**
     * 打印异常的message.
     * 
     * @param throwable
     *            异常
     * @param writer
     *            输出流
     * @param cause
     *            是否是起因异常
     */
    private static void printThrowableMessage(ChainedThrowable throwable, PrintWriter writer, boolean cause) {
        StringBuffer buffer = new StringBuffer();

        if (cause) {
            buffer.append(STRING_CAUSED_BY);
        }

        Throwable t = getThrowable(throwable);

        buffer.append(t.getClass().getName());

        String message = t.getMessage();

        if (message != null) {
            buffer.append(STRING_EXCEPTION_MESSAGE).append(message);
        }

        writer.println(buffer.toString());
    }


    /**
     * 分析异常的调用栈, 取得当前异常的信息, 不包括起因异常的信息.
     * 
     * @param throwable
     *            取得指定异常的调用栈
     * 
     * @return 调用栈数组
     */
    private static String[] analyzeStackTrace(ChainedThrowable throwable) {
        if (GET_STACK_TRACE_METHOD != null) {
            Throwable t = getThrowable(throwable);

            try {
                Object[] stackTrace = (Object[]) GET_STACK_TRACE_METHOD.invoke(t, new Object[0]);
                String[] list = new String[stackTrace.length];

                for (int i = 0; i < stackTrace.length; i++) {
                    list[i] = STRING_STACK_TRACE_PREFIX + stackTrace[i];
                }

                return list;
            }
            catch (IllegalAccessException e) {
            }
            catch (IllegalArgumentException e) {
            }
            catch (InvocationTargetException e) {
            }
        }

        return new StackTraceAnalyzer(throwable).getLines();
    }

    /**
     * 分析stack trace的辅助类.
     */
    private static class StackTraceAnalyzer {
        private Throwable throwable;
        private String message;
        private StackTraceEntry currentEntry = new StackTraceEntry();
        private StackTraceEntry selectedEntry = currentEntry;
        private StackTraceEntry entry;


        StackTraceAnalyzer(ChainedThrowable throwable) {
            this.throwable = getThrowable(throwable);
            this.message = this.throwable.getMessage();

            // 取得stack trace字符串.
            StringWriter writer = new StringWriter();
            PrintWriter pw = new PrintWriter(writer);

            throwable.printCurrentStackTrace(pw);

            String stackTraceDump = writer.toString();

            // 分割字符串, 按行分割, 但不能割开message字串
            int p = 0;
            int i = -1;
            int j = -1;
            int k = -1;

            while (p < stackTraceDump.length()) {
                boolean includesMessage = false;
                int s = p;

                if ((i == -1) && (message != null)) {
                    i = stackTraceDump.indexOf(message, p);
                }

                if (j == -1) {
                    j = stackTraceDump.indexOf(STRING_CR, p);
                }

                if (k == -1) {
                    k = stackTraceDump.indexOf(STRING_LF, p);
                }

                // 如果找到message
                if ((i != -1) && ((j == -1) || (i <= j)) && ((k == -1) || (i <= k))) {
                    includesMessage = true;
                    p = i + message.length();
                    i = -1;

                    if (j < p) {
                        j = -1;
                    }

                    if (k < p) {
                        k = -1;
                    }

                    // 继续直到换行
                }

                // 如果找到换行CR或CRLF
                if ((j != -1) && ((k == -1) || (j < k))) {
                    p = j + 1;

                    if ((p < stackTraceDump.length()) && (stackTraceDump.charAt(p) == STRING_LF.charAt(0))) {
                        p++; // CRLF
                    }

                    addLine(stackTraceDump.substring(s, j), includesMessage);

                    j = -1;

                    if (k < p) {
                        k = -1;
                    }

                    continue;
                }

                // 如果找到LF
                if (k != -1) {
                    int q = k + 1;

                    addLine(stackTraceDump.substring(s, k), includesMessage);
                    p = q;
                    k = -1;
                    continue;
                }

                // 如果都没找到, 说明到了最后一行
                int q = stackTraceDump.length();

                if ((p + 1) < q) {
                    addLine(stackTraceDump.substring(s), includesMessage);
                    p = q;
                }
            }

            // 选择"较小"的entry
            if (currentEntry.compareTo(selectedEntry) < 0) {
                selectedEntry = currentEntry;
            }
        }


        private void addLine(String line, boolean includesMessage) {
            StackTraceEntry nextEntry = currentEntry.accept(line, includesMessage);

            if (nextEntry != null) {
                // 选择"较小"的entry
                if (currentEntry.compareTo(selectedEntry) < 0) {
                    selectedEntry = currentEntry;
                }

                currentEntry = nextEntry;
            }
        }


        String[] getLines() {
            return (String[]) selectedEntry.lines.toArray(new String[selectedEntry.lines.size()]);
        }

        private class StackTraceEntry implements Comparable {
            private List lines = new ArrayList(10);
            private int includesMessage = 0;
            private int includesThrowable = 0;
            private int count = 0;


            StackTraceEntry accept(String line, boolean includesMessage) {
                // 如果是...at XXX.java(Line...), 则加入到lines列表中.
                // 否则创建并返回新的entry.
                if (line.startsWith(STRING_STACK_TRACE_PREFIX)) {
                    lines.add(line);
                    count++;
                    return null;
                }
                else if (count > 0) {
                    StackTraceEntry newEntry = new StackTraceEntry();

                    newEntry.accept(line, includesMessage);
                    return newEntry;
                }

                // 设置权重
                if (includesMessage) {
                    this.includesMessage = 1;
                }

                if (line.indexOf(throwable.getClass().getName()) != -1) {
                    this.includesThrowable = 1;
                }

                return null;
            }


            public int compareTo(Object o) {
                StackTraceEntry otherEntry = (StackTraceEntry) o;
                int thisWeight = includesMessage + includesThrowable;
                int otherWeight = otherEntry.includesMessage + otherEntry.includesThrowable;

                // weight大的排在前, 如果weight相同, 则count小的排在前
                if (thisWeight == otherWeight) {
                    return count - otherEntry.count;
                }
                else {
                    return otherWeight - thisWeight;
                }
            }
        }
    }
}
