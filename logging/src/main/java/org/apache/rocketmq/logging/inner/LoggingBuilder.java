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

package org.apache.rocketmq.logging.inner;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FilterWriter;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.text.MessageFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.Enumeration;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TimeZone;

public class LoggingBuilder {

    public static final String SYSTEM_OUT = "System.out";
    public static final String SYSTEM_ERR = "System.err";

    public static final String LOGGING_ENCODING = "rocketmq.logging.inner.encoding";
    public static final String ENCODING = System.getProperty(LOGGING_ENCODING, "UTF-8");

    public static AppenderBuilder newAppenderBuilder() {
        return new AppenderBuilder();
    }

    public static class AppenderBuilder {
        private AsyncAppender asyncAppender;

        private Appender appender = null;

        private AppenderBuilder() {

        }

        public AppenderBuilder withLayout(Layout layout) {
            appender.setLayout(layout);
            return this;
        }

        public AppenderBuilder withName(String name) {
            appender.setName(name);
            return this;
        }

        public AppenderBuilder withConsoleAppender(String target) {
            ConsoleAppender consoleAppender = new ConsoleAppender();
            consoleAppender.setTarget(target);
            consoleAppender.activateOptions();
            this.appender = consoleAppender;
            return this;
        }

        public AppenderBuilder withFileAppender(String file) {
            FileAppender appender = new FileAppender();
            appender.setFile(file);
            appender.setAppend(true);
            appender.setBufferedIO(false);
            appender.setEncoding(ENCODING);
            appender.setImmediateFlush(true);
            appender.activateOptions();
            this.appender = appender;
            return this;
        }

        public AppenderBuilder withRollingFileAppender(String file, String maxFileSize, int maxFileIndex) {
            RollingFileAppender appender = new RollingFileAppender();
            appender.setFile(file);
            appender.setAppend(true);
            appender.setBufferedIO(false);
            appender.setEncoding(ENCODING);
            appender.setImmediateFlush(true);
            appender.setMaximumFileSize(Integer.parseInt(maxFileSize));
            appender.setMaxBackupIndex(maxFileIndex);
            appender.activateOptions();
            this.appender = appender;
            return this;
        }

        public AppenderBuilder withDailyFileRollingAppender(String file, String datePattern) {
            DailyRollingFileAppender appender = new DailyRollingFileAppender();
            appender.setFile(file);
            appender.setAppend(true);
            appender.setBufferedIO(false);
            appender.setEncoding(ENCODING);
            appender.setImmediateFlush(true);
            appender.setDatePattern(datePattern);
            appender.activateOptions();
            this.appender = appender;
            return this;
        }

        public AppenderBuilder withAsync(boolean blocking, int buffSize) {
            AsyncAppender asyncAppender = new AsyncAppender();
            asyncAppender.setBlocking(blocking);
            asyncAppender.setBufferSize(buffSize);
            this.asyncAppender = asyncAppender;
            return this;
        }

        public Appender build() {
            if (appender == null) {
                throw new RuntimeException("please specify appender first");
            }
            if (asyncAppender != null) {
                asyncAppender.addAppender(appender);
                return asyncAppender;
            } else {
                return appender;
            }
        }
    }

    public static class AsyncAppender extends Appender implements Appender.AppenderPipeline {

        public static final int DEFAULT_BUFFER_SIZE = 128;

        private final List<LoggingEvent> buffer = new ArrayList<LoggingEvent>();

        private final Map<String, DiscardSummary> discardMap = new HashMap<String, DiscardSummary>();

        private int bufferSize = DEFAULT_BUFFER_SIZE;

        private final AppenderPipelineImpl appenderPipeline;

        private final Thread dispatcher;

        private boolean blocking = true;

        public AsyncAppender() {
            appenderPipeline = new AppenderPipelineImpl();

            dispatcher = new Thread(new Dispatcher(this, buffer, discardMap, appenderPipeline));

            dispatcher.setDaemon(true);

            dispatcher.setName("AsyncAppender-Dispatcher-" + dispatcher.getName());
            dispatcher.start();
        }

        public void addAppender(final Appender newAppender) {
            synchronized (appenderPipeline) {
                appenderPipeline.addAppender(newAppender);
            }
        }

        public void append(final LoggingEvent event) {
            if ((dispatcher == null) || !dispatcher.isAlive() || (bufferSize <= 0)) {
                synchronized (appenderPipeline) {
                    appenderPipeline.appendLoopOnAppenders(event);
                }

                return;
            }

            event.getThreadName();
            event.getRenderedMessage();

            synchronized (buffer) {
                while (true) {
                    int previousSize = buffer.size();

                    if (previousSize < bufferSize) {
                        buffer.add(event);

                        if (previousSize == 0) {
                            buffer.notifyAll();
                        }

                        break;
                    }

                    boolean discard = true;
                    if (blocking
                        && !Thread.interrupted()
                        && Thread.currentThread() != dispatcher) {
                        try {
                            buffer.wait();
                            discard = false;
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }
                    }
                    if (discard) {
                        String loggerName = event.getLoggerName();
                        DiscardSummary summary = discardMap.get(loggerName);

                        if (summary == null) {
                            summary = new DiscardSummary(event);
                            discardMap.put(loggerName, summary);
                        } else {
                            summary.add(event);
                        }

                        break;
                    }
                }
            }
        }

        public void close() {

            synchronized (buffer) {
                closed = true;
                buffer.notifyAll();
            }

            try {
                dispatcher.join();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                SysLogger.error(
                    "Got an InterruptedException while waiting for the "
                        + "dispatcher to finish.", e);
            }

            synchronized (appenderPipeline) {
                Enumeration iter = appenderPipeline.getAllAppenders();
                if (iter != null) {
                    while (iter.hasMoreElements()) {
                        Object next = iter.nextElement();
                        if (next instanceof Appender) {
                            ((Appender) next).close();
                        }
                    }
                }
            }
        }

        public Enumeration getAllAppenders() {
            synchronized (appenderPipeline) {
                return appenderPipeline.getAllAppenders();
            }
        }

        public Appender getAppender(final String name) {
            synchronized (appenderPipeline) {
                return appenderPipeline.getAppender(name);
            }
        }

        public boolean isAttached(final Appender appender) {
            synchronized (appenderPipeline) {
                return appenderPipeline.isAttached(appender);
            }
        }

        public void removeAllAppenders() {
            synchronized (appenderPipeline) {
                appenderPipeline.removeAllAppenders();
            }
        }

        public void removeAppender(final Appender appender) {
            synchronized (appenderPipeline) {
                appenderPipeline.removeAppender(appender);
            }
        }

        public void removeAppender(final String name) {
            synchronized (appenderPipeline) {
                appenderPipeline.removeAppender(name);
            }
        }

        public void setBufferSize(final int size) {
            if (size < 0) {
                throw new NegativeArraySizeException("size");
            }

            synchronized (buffer) {
                bufferSize = (size < 1) ? 1 : size;
                buffer.notifyAll();
            }
        }

        public int getBufferSize() {
            return bufferSize;
        }

        public void setBlocking(final boolean value) {
            synchronized (buffer) {
                blocking = value;
                buffer.notifyAll();
            }
        }

        public boolean getBlocking() {
            return blocking;
        }

        private final class DiscardSummary {

            private LoggingEvent maxEvent;

            private int count;

            public DiscardSummary(final LoggingEvent event) {
                maxEvent = event;
                count = 1;
            }

            public void add(final LoggingEvent event) {
                if (event.getLevel().toInt() > maxEvent.getLevel().toInt()) {
                    maxEvent = event;
                }
                count++;
            }

            public LoggingEvent createEvent() {
                String msg =
                    MessageFormat.format(
                        "Discarded {0} messages due to full event buffer including: {1}",
                        count, maxEvent.getMessage());

                return new LoggingEvent(
                    "AsyncAppender.DONT_REPORT_LOCATION",
                    Logger.getLogger(maxEvent.getLoggerName()),
                    maxEvent.getLevel(),
                    msg,
                    null);
            }
        }

        private class Dispatcher implements Runnable {

            private final AsyncAppender parent;

            private final List<LoggingEvent> buffer;

            private final Map<String, DiscardSummary> discardMap;

            private final AppenderPipelineImpl appenderPipeline;

            public Dispatcher(
                final AsyncAppender parent, final List<LoggingEvent> buffer, final Map<String, DiscardSummary> discardMap,
                final AppenderPipelineImpl appenderPipeline) {

                this.parent = parent;
                this.buffer = buffer;
                this.appenderPipeline = appenderPipeline;
                this.discardMap = discardMap;
            }

            public void run() {
                boolean isActive = true;

                try {
                    while (isActive) {
                        LoggingEvent[] events = null;

                        synchronized (buffer) {
                            int bufferSize = buffer.size();
                            isActive = !parent.closed;

                            while ((bufferSize == 0) && isActive) {
                                buffer.wait();
                                bufferSize = buffer.size();
                                isActive = !parent.closed;
                            }

                            if (bufferSize > 0) {
                                events = new LoggingEvent[bufferSize + discardMap.size()];
                                buffer.toArray(events);

                                int index = bufferSize;
                                Collection<DiscardSummary> values = discardMap.values();
                                for (DiscardSummary value : values) {
                                    events[index++] = value.createEvent();
                                }

                                buffer.clear();
                                discardMap.clear();

                                buffer.notifyAll();
                            }
                        }
                        if (events != null) {
                            for (LoggingEvent event : events) {
                                synchronized (appenderPipeline) {
                                    appenderPipeline.appendLoopOnAppenders(event);
                                }
                            }
                        }
                    }
                } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                }
            }
        }
    }

    private static class QuietWriter extends FilterWriter {

        protected Appender appender;

        public QuietWriter(Writer writer, Appender appender) {
            super(writer);
            this.appender = appender;
        }

        public void write(String string) {
            if (string != null) {
                try {
                    out.write(string);
                } catch (Exception e) {
                    appender.handleError("Failed to write [" + string + "].", e,
                        Appender.CODE_WRITE_FAILURE);
                }
            }
        }

        public void flush() {
            try {
                out.flush();
            } catch (Exception e) {
                appender.handleError("Failed to flush writer,", e,
                    Appender.CODE_FLUSH_FAILURE);
            }
        }
    }

    public static class WriterAppender extends Appender {


        protected boolean immediateFlush = true;

        protected String encoding;


        protected QuietWriter qw;

        public WriterAppender() {

        }

        public void setImmediateFlush(boolean value) {
            immediateFlush = value;
        }


        public boolean getImmediateFlush() {
            return immediateFlush;
        }

        public void activateOptions() {
        }


        public void append(LoggingEvent event) {
            if (!checkEntryConditions()) {
                return;
            }
            subAppend(event);
        }

        protected boolean checkEntryConditions() {
            if (this.closed) {
                SysLogger.warn("Not allowed to write to a closed appender.");
                return false;
            }

            if (this.qw == null) {
                handleError("No output stream or file set for the appender named [" +
                    name + "].");
                return false;
            }

            if (this.layout == null) {
                handleError("No layout set for the appender named [" + name + "].");
                return false;
            }
            return true;
        }

        public synchronized void close() {
            if (this.closed) {
                return;
            }
            this.closed = true;
            writeFooter();
            reset();
        }

        protected void closeWriter() {
            if (qw != null) {
                try {
                    qw.close();
                } catch (IOException e) {
                    handleError("Could not close " + qw, e, CODE_CLOSE_FAILURE);
                }
            }
        }

        protected OutputStreamWriter createWriter(OutputStream os) {
            OutputStreamWriter retval = null;

            String enc = getEncoding();
            if (enc != null) {
                try {
                    retval = new OutputStreamWriter(os, enc);
                } catch (IOException e) {
                    SysLogger.warn("Error initializing output writer.");
                    SysLogger.warn("Unsupported encoding?");
                }
            }
            if (retval == null) {
                retval = new OutputStreamWriter(os);
            }
            return retval;
        }

        public String getEncoding() {
            return encoding;
        }

        public void setEncoding(String value) {
            encoding = value;
        }


        public synchronized void setWriter(Writer writer) {
            reset();
            this.qw = new QuietWriter(writer, this);
            writeHeader();
        }

        protected void subAppend(LoggingEvent event) {
            this.qw.write(this.layout.format(event));

            if (layout.ignoresThrowable()) {
                String[] s = event.getThrowableStr();
                if (s != null) {
                    for (String s1 : s) {
                        this.qw.write(s1);
                        this.qw.write(LINE_SEP);
                    }
                }
            }

            if (shouldFlush(event)) {
                this.qw.flush();
            }
        }

        protected void reset() {
            closeWriter();
            this.qw = null;
        }

        protected void writeFooter() {
            if (layout != null) {
                String f = layout.getFooter();
                if (f != null && this.qw != null) {
                    this.qw.write(f);
                    this.qw.flush();
                }
            }
        }

        protected void writeHeader() {
            if (layout != null) {
                String h = layout.getHeader();
                if (h != null && this.qw != null) {
                    this.qw.write(h);
                }
            }
        }

        protected boolean shouldFlush(final LoggingEvent event) {
            return event != null && immediateFlush;
        }
    }


    public static class FileAppender extends WriterAppender {

        protected boolean fileAppend = true;

        protected String fileName = null;

        protected boolean bufferedIO = false;

        protected int bufferSize = 8 * 1024;

        public FileAppender() {
        }

        public FileAppender(Layout layout, String filename, boolean append)
            throws IOException {
            this.layout = layout;
            this.setFile(filename, append, false, bufferSize);
        }

        public void setFile(String file) {
            fileName = file.trim();
        }

        public boolean getAppend() {
            return fileAppend;
        }

        public String getFile() {
            return fileName;
        }

        public void activateOptions() {
            if (fileName != null) {
                try {
                    setFile(fileName, fileAppend, bufferedIO, bufferSize);
                } catch (IOException e) {
                    handleError("setFile(" + fileName + "," + fileAppend + ") call failed.",
                        e, CODE_FILE_OPEN_FAILURE);
                }
            } else {
                SysLogger.warn("File option not set for appender [" + name + "].");
                SysLogger.warn("Are you using FileAppender instead of ConsoleAppender?");
            }
        }

        protected void closeFile() {
            if (this.qw != null) {
                try {
                    this.qw.close();
                } catch (IOException e) {
                    if (e instanceof InterruptedIOException) {
                        Thread.currentThread().interrupt();
                    }
                    SysLogger.error("Could not close " + qw, e);
                }
            }
        }

        public boolean getBufferedIO() {
            return this.bufferedIO;
        }

        public int getBufferSize() {
            return this.bufferSize;
        }

        public void setAppend(boolean flag) {
            fileAppend = flag;
        }

        public void setBufferedIO(boolean bufferedIO) {
            this.bufferedIO = bufferedIO;
            if (bufferedIO) {
                immediateFlush = false;
            }
        }

        public void setBufferSize(int bufferSize) {
            this.bufferSize = bufferSize;
        }

        public synchronized void setFile(String fileName, boolean append, boolean bufferedIO, int bufferSize)
            throws IOException {
            SysLogger.debug("setFile called: " + fileName + ", " + append);

            if (bufferedIO) {
                setImmediateFlush(false);
            }

            reset();
            FileOutputStream ostream;
            try {
                ostream = new FileOutputStream(fileName, append);
            } catch (FileNotFoundException ex) {
                String parentName = new File(fileName).getParent();
                if (parentName != null) {
                    File parentDir = new File(parentName);
                    if (!parentDir.exists() && parentDir.mkdirs()) {
                        ostream = new FileOutputStream(fileName, append);
                    } else {
                        throw ex;
                    }
                } else {
                    throw ex;
                }
            }
            Writer fw = createWriter(ostream);
            if (bufferedIO) {
                fw = new BufferedWriter(fw, bufferSize);
            }
            this.setQWForFiles(fw);
            this.fileName = fileName;
            this.fileAppend = append;
            this.bufferedIO = bufferedIO;
            this.bufferSize = bufferSize;
            writeHeader();
            SysLogger.debug("setFile ended");
        }

        protected void setQWForFiles(Writer writer) {
            this.qw = new QuietWriter(writer, this);
        }

        protected void reset() {
            closeFile();
            this.fileName = null;
            super.reset();
        }
    }


    public static class RollingFileAppender extends FileAppender {

        protected long maxFileSize = 10 * 1024 * 1024;

        protected int maxBackupIndex = 1;

        private long nextRollover = 0;

        public RollingFileAppender() {
            super();
        }

        public int getMaxBackupIndex() {
            return maxBackupIndex;
        }

        public long getMaximumFileSize() {
            return maxFileSize;
        }

        public void rollOver() {
            File target;
            File file;

            if (qw != null) {
                long size = ((CountingQuietWriter) qw).getCount();
                SysLogger.debug("rolling over count=" + size);
                nextRollover = size + maxFileSize;
            }
            SysLogger.debug("maxBackupIndex=" + maxBackupIndex);

            boolean renameSucceeded = true;
            if (maxBackupIndex > 0) {
                file = new File(fileName + '.' + maxBackupIndex);
                if (file.exists()) {
                    renameSucceeded = file.delete();
                }

                for (int i = maxBackupIndex - 1; i >= 1 && renameSucceeded; i--) {
                    file = new File(fileName + "." + i);
                    if (file.exists()) {
                        target = new File(fileName + '.' + (i + 1));
                        SysLogger.debug("Renaming file " + file + " to " + target);
                        renameSucceeded = file.renameTo(target);
                    }
                }

                if (renameSucceeded) {
                    target = new File(fileName + "." + 1);

                    this.closeFile(); // keep windows happy.

                    file = new File(fileName);
                    SysLogger.debug("Renaming file " + file + " to " + target);
                    renameSucceeded = file.renameTo(target);

                    if (!renameSucceeded) {
                        try {
                            this.setFile(fileName, true, bufferedIO, bufferSize);
                        } catch (IOException e) {
                            if (e instanceof InterruptedIOException) {
                                Thread.currentThread().interrupt();
                            }
                            SysLogger.error("setFile(" + fileName + ", true) call failed.", e);
                        }
                    }
                }
            }

            if (renameSucceeded) {
                try {
                    this.setFile(fileName, false, bufferedIO, bufferSize);
                    nextRollover = 0;
                } catch (IOException e) {
                    if (e instanceof InterruptedIOException) {
                        Thread.currentThread().interrupt();
                    }
                    SysLogger.error("setFile(" + fileName + ", false) call failed.", e);
                }
            }
        }

        public synchronized void setFile(String fileName, boolean append, boolean bufferedIO, int bufferSize)
            throws IOException {
            super.setFile(fileName, append, this.bufferedIO, this.bufferSize);
            if (append) {
                File f = new File(fileName);
                ((CountingQuietWriter) qw).setCount(f.length());
            }
        }

        public void setMaxBackupIndex(int maxBackups) {
            this.maxBackupIndex = maxBackups;
        }

        public void setMaximumFileSize(long maxFileSize) {
            this.maxFileSize = maxFileSize;
        }

        protected void setQWForFiles(Writer writer) {
            this.qw = new CountingQuietWriter(writer, this);
        }

        protected void subAppend(LoggingEvent event) {
            super.subAppend(event);
            if (fileName != null && qw != null) {
                long size = ((CountingQuietWriter) qw).getCount();
                if (size >= maxFileSize && size >= nextRollover) {
                    rollOver();
                }
            }
        }

        protected class CountingQuietWriter extends QuietWriter {

            protected long count;

            public CountingQuietWriter(Writer writer, Appender appender) {
                super(writer, appender);
            }

            public void write(String string) {
                try {
                    out.write(string);
                    count += string.length();
                } catch (IOException e) {
                    appender.handleError("Write failure.", e, Appender.CODE_WRITE_FAILURE);
                }
            }

            public long getCount() {
                return count;
            }

            public void setCount(long count) {
                this.count = count;
            }

        }
    }


    public static class DailyRollingFileAppender extends FileAppender {

        static final int TOP_OF_TROUBLE = -1;
        static final int TOP_OF_MINUTE = 0;
        static final int TOP_OF_HOUR = 1;
        static final int HALF_DAY = 2;
        static final int TOP_OF_DAY = 3;
        static final int TOP_OF_WEEK = 4;
        static final int TOP_OF_MONTH = 5;


        /**
         * The date pattern. By default, the pattern is set to
         * "'.'yyyy-MM-dd" meaning daily rollover.
         */
        private String datePattern = "'.'yyyy-MM-dd";

        private String scheduledFilename;

        private long nextCheck = System.currentTimeMillis() - 1;

        Date now = new Date();

        SimpleDateFormat sdf;

        RollingCalendar rc = new RollingCalendar();

        final TimeZone gmtTimeZone = TimeZone.getTimeZone("GMT");


        public void setDatePattern(String pattern) {
            datePattern = pattern;
        }

        public String getDatePattern() {
            return datePattern;
        }

        public void activateOptions() {
            super.activateOptions();
            if (datePattern != null && fileName != null) {
                now.setTime(System.currentTimeMillis());
                sdf = new SimpleDateFormat(datePattern);
                int type = computeCheckPeriod();
                printPeriodicity(type);
                rc.setType(type);
                File file = new File(fileName);
                scheduledFilename = fileName + sdf.format(new Date(file.lastModified()));

            } else {
                SysLogger.error("Either File or DatePattern options are not set for appender [" + name + "].");
            }
        }

        void printPeriodicity(int type) {
            switch (type) {
                case TOP_OF_MINUTE:
                    SysLogger.debug("Appender [" + name + "] to be rolled every minute.");
                    break;
                case TOP_OF_HOUR:
                    SysLogger.debug("Appender [" + name + "] to be rolled on top of every hour.");
                    break;
                case HALF_DAY:
                    SysLogger.debug("Appender [" + name + "] to be rolled at midday and midnight.");
                    break;
                case TOP_OF_DAY:
                    SysLogger.debug("Appender [" + name + "] to be rolled at midnight.");
                    break;
                case TOP_OF_WEEK:
                    SysLogger.debug("Appender [" + name + "] to be rolled at start of week.");
                    break;
                case TOP_OF_MONTH:
                    SysLogger.debug("Appender [" + name + "] to be rolled at start of every month.");
                    break;
                default:
                    SysLogger.warn("Unknown periodicity for appender [" + name + "].");
            }
        }

        int computeCheckPeriod() {
            RollingCalendar rollingCalendar = new RollingCalendar(gmtTimeZone, Locale.getDefault());
            // set sate to 1970-01-01 00:00:00 GMT
            Date epoch = new Date(0);
            if (datePattern != null) {
                for (int i = TOP_OF_MINUTE; i <= TOP_OF_MONTH; i++) {
                    SimpleDateFormat simpleDateFormat = new SimpleDateFormat(datePattern);
                    simpleDateFormat.setTimeZone(gmtTimeZone);
                    String r0 = simpleDateFormat.format(epoch);
                    rollingCalendar.setType(i);
                    Date next = new Date(rollingCalendar.getNextCheckMillis(epoch));
                    String r1 = simpleDateFormat.format(next);
                    if (r0 != null && r1 != null && !r0.equals(r1)) {
                        return i;
                    }
                }
            }
            return TOP_OF_TROUBLE;
        }

        void rollOver() throws IOException {

            if (datePattern == null) {
                handleError("Missing DatePattern option in rollOver().");
                return;
            }

            String datedFilename = fileName + sdf.format(now);

            if (scheduledFilename.equals(datedFilename)) {
                return;
            }
            this.closeFile();

            File target = new File(scheduledFilename);
            if (target.exists() && !target.delete()) {
                SysLogger.error("Failed to delete [" + scheduledFilename + "].");
            }

            File file = new File(fileName);
            boolean result = file.renameTo(target);
            if (result) {
                SysLogger.debug(fileName + " -> " + scheduledFilename);
            } else {
                SysLogger.error("Failed to rename [" + fileName + "] to [" + scheduledFilename + "].");
            }

            try {
                this.setFile(fileName, true, this.bufferedIO, this.bufferSize);
            } catch (IOException e) {
                handleError("setFile(" + fileName + ", true) call failed.");
            }
            scheduledFilename = datedFilename;
        }

        protected void subAppend(LoggingEvent event) {
            long n = System.currentTimeMillis();
            if (n >= nextCheck) {
                now.setTime(n);
                nextCheck = rc.getNextCheckMillis(now);
                try {
                    rollOver();
                } catch (IOException ioe) {
                    if (ioe instanceof InterruptedIOException) {
                        Thread.currentThread().interrupt();
                    }
                    SysLogger.error("rollOver() failed.", ioe);
                }
            }
            super.subAppend(event);
        }
    }

    private static class RollingCalendar extends GregorianCalendar {
        private static final long serialVersionUID = -3560331770601814177L;

        int type = DailyRollingFileAppender.TOP_OF_TROUBLE;

        RollingCalendar() {
            super();
        }

        RollingCalendar(TimeZone tz, Locale locale) {
            super(tz, locale);
        }

        void setType(int type) {
            this.type = type;
        }

        public long getNextCheckMillis(Date now) {
            return getNextCheckDate(now).getTime();
        }

        public Date getNextCheckDate(Date now) {
            this.setTime(now);

            switch (type) {
                case DailyRollingFileAppender.TOP_OF_MINUTE:
                    this.set(Calendar.SECOND, 0);
                    this.set(Calendar.MILLISECOND, 0);
                    this.add(Calendar.MINUTE, 1);
                    break;
                case DailyRollingFileAppender.TOP_OF_HOUR:
                    this.set(Calendar.MINUTE, 0);
                    this.set(Calendar.SECOND, 0);
                    this.set(Calendar.MILLISECOND, 0);
                    this.add(Calendar.HOUR_OF_DAY, 1);
                    break;
                case DailyRollingFileAppender.HALF_DAY:
                    this.set(Calendar.MINUTE, 0);
                    this.set(Calendar.SECOND, 0);
                    this.set(Calendar.MILLISECOND, 0);
                    int hour = get(Calendar.HOUR_OF_DAY);
                    if (hour < 12) {
                        this.set(Calendar.HOUR_OF_DAY, 12);
                    } else {
                        this.set(Calendar.HOUR_OF_DAY, 0);
                        this.add(Calendar.DAY_OF_MONTH, 1);
                    }
                    break;
                case DailyRollingFileAppender.TOP_OF_DAY:
                    this.set(Calendar.HOUR_OF_DAY, 0);
                    this.set(Calendar.MINUTE, 0);
                    this.set(Calendar.SECOND, 0);
                    this.set(Calendar.MILLISECOND, 0);
                    this.add(Calendar.DATE, 1);
                    break;
                case DailyRollingFileAppender.TOP_OF_WEEK:
                    this.set(Calendar.DAY_OF_WEEK, getFirstDayOfWeek());
                    this.set(Calendar.HOUR_OF_DAY, 0);
                    this.set(Calendar.MINUTE, 0);
                    this.set(Calendar.SECOND, 0);
                    this.set(Calendar.MILLISECOND, 0);
                    this.add(Calendar.WEEK_OF_YEAR, 1);
                    break;
                case DailyRollingFileAppender.TOP_OF_MONTH:
                    this.set(Calendar.DATE, 1);
                    this.set(Calendar.HOUR_OF_DAY, 0);
                    this.set(Calendar.MINUTE, 0);
                    this.set(Calendar.SECOND, 0);
                    this.set(Calendar.MILLISECOND, 0);
                    this.add(Calendar.MONTH, 1);
                    break;
                default:
                    throw new IllegalStateException("Unknown periodicity type.");
            }
            return getTime();
        }
    }

    public static class ConsoleAppender extends WriterAppender {

        protected String target = SYSTEM_OUT;

        public ConsoleAppender() {
        }

        public void setTarget(String value) {
            String v = value.trim();

            if (SYSTEM_OUT.equalsIgnoreCase(v)) {
                target = SYSTEM_OUT;
            } else if (SYSTEM_ERR.equalsIgnoreCase(v)) {
                target = SYSTEM_ERR;
            } else {
                targetWarn(value);
            }
        }

        public String getTarget() {
            return target;
        }

        void targetWarn(String val) {
            SysLogger.warn("[" + val + "] should be System.out or System.err.");
            SysLogger.warn("Using previously set target, System.out by default.");
        }

        public void activateOptions() {
            if (target.equals(SYSTEM_ERR)) {
                setWriter(createWriter(System.err));
            } else {
                setWriter(createWriter(System.out));
            }
            super.activateOptions();
        }

        protected final void closeWriter() {

        }
    }

    public static LayoutBuilder newLayoutBuilder() {
        return new LayoutBuilder();
    }

    public static class LayoutBuilder {

        private Layout layout;

        public LayoutBuilder withSimpleLayout() {
            layout = new SimpleLayout();
            return this;
        }

        public LayoutBuilder withDefaultLayout() {
            layout = new DefaultLayout();
            return this;
        }

        public Layout build() {
            if (layout == null) {
                layout = new SimpleLayout();
            }
            return layout;
        }
    }

    public static class SimpleLayout extends Layout {

        @Override
        public String format(LoggingEvent event) {

            StringBuilder sb = new StringBuilder();
            sb.append(event.getLevel().toString());
            sb.append(" - ");
            sb.append(event.getRenderedMessage());
            sb.append("\r\n");
            return sb.toString();
        }

        @Override
        public boolean ignoresThrowable() {
            return false;
        }
    }


    /**
     * %d{yyy-MM-dd HH:mm:ss,SSS} %p %c{1}%L - %m%n
     */
    public static class DefaultLayout extends Layout {
        @Override
        public String format(LoggingEvent event) {

            StringBuilder sb = new StringBuilder();
            SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss,SSS");
            String format = simpleDateFormat.format(new Date(event.timeStamp));
            sb.append(format);
            sb.append(" ");
            sb.append(event.getLevel());
            sb.append(" ");
            sb.append(event.getLoggerName());
            sb.append(" - ");
            sb.append(event.getRenderedMessage());
            String[] throwableStr = event.getThrowableStr();
            if (throwableStr != null) {
                sb.append("\r\n");
                for (String s : throwableStr) {
                    sb.append(s);
                    sb.append("\r\n");
                }
            }
            sb.append("\r\n");
            return sb.toString();
        }

        @Override
        public boolean ignoresThrowable() {
            return false;
        }
    }
}
