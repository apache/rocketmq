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

package org.apache.rocketmq.common.logging.internal;

import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Vector;


public class Logger implements Appender.AppenderPipeline {

    private static final String FQCN = Logger.class.getName();

    private static final DefaultLoggerRepository REPOSITORY = new DefaultLoggerRepository(new RootLogger(Level.DEBUG));

    public static LoggerRepository getRepository() {
        return REPOSITORY;
    }

    private String name;

    volatile private Level level;

    volatile private Logger parent;

    Appender.AppenderPipelineImpl appenderPipeline;

    private boolean additive = true;

    private Logger(String name) {
        this.name = name;
    }

    static public Logger getLogger(String name) {
        return getRepository().getLogger(name);
    }

    static public Logger getLogger(Class clazz) {
        return getRepository().getLogger(clazz.getName());
    }

    public static Logger getRootLogger() {
        return getRepository().getRootLogger();
    }

    synchronized public void addAppender(Appender newAppender) {
        if (appenderPipeline == null) {
            appenderPipeline = new Appender.AppenderPipelineImpl();
        }
        appenderPipeline.addAppender(newAppender);
    }

    public void callAppenders(LoggingEvent event) {
        int writes = 0;

        for (Logger c = this; c != null; c = c.parent) {
            synchronized (c) {
                if (c.appenderPipeline != null) {
                    writes += c.appenderPipeline.appendLoopOnAppenders(event);
                }
                if (!c.additive) {
                    break;
                }
            }
        }

        if (writes == 0) {
            getRepository().emitNoAppenderWarning(this);
        }
    }

    synchronized void closeNestedAppenders() {
        Enumeration enumeration = this.getAllAppenders();
        if (enumeration != null) {
            while (enumeration.hasMoreElements()) {
                Appender a = (Appender) enumeration.nextElement();
                if (a instanceof Appender.AppenderPipeline) {
                    a.close();
                }
            }
        }
    }

    public void debug(Object message) {
        if (getRepository().isDisabled(Level.DEBUG_INT)) {
            return;
        }
        if (Level.DEBUG.isGreaterOrEqual(this.getEffectiveLevel())) {
            forcedLog(FQCN, Level.DEBUG, message, null);
        }
    }


    public void debug(Object message, Throwable t) {
        if (getRepository().isDisabled(Level.DEBUG_INT)) {
            return;
        }
        if (Level.DEBUG.isGreaterOrEqual(this.getEffectiveLevel())) {
            forcedLog(FQCN, Level.DEBUG, message, t);
        }
    }


    public void error(Object message) {
        if (getRepository().isDisabled(Level.ERROR_INT)) {
            return;
        }
        if (Level.ERROR.isGreaterOrEqual(this.getEffectiveLevel())) {
            forcedLog(FQCN, Level.ERROR, message, null);
        }
    }

    public void error(Object message, Throwable t) {
        if (getRepository().isDisabled(Level.ERROR_INT)) {
            return;
        }
        if (Level.ERROR.isGreaterOrEqual(this.getEffectiveLevel())) {
            forcedLog(FQCN, Level.ERROR, message, t);
        }

    }


    protected void forcedLog(String fqcn, Level level, Object message, Throwable t) {
        callAppenders(new LoggingEvent(fqcn, this, level, message, t));
    }


    synchronized public Enumeration getAllAppenders() {
        if (appenderPipeline == null) {
            return null;
        } else {
            return appenderPipeline.getAllAppenders();
        }
    }

    synchronized public Appender getAppender(String name) {
        if (appenderPipeline == null || name == null) {
            return null;
        }

        return appenderPipeline.getAppender(name);
    }

    public Level getEffectiveLevel() {
        for (Logger c = this; c != null; c = c.parent) {
            if (c.level != null) {
                return c.level;
            }
        }
        return null;
    }

    public final String getName() {
        return name;
    }

    final public Level getLevel() {
        return this.level;
    }


    public void info(Object message) {
        if (getRepository().isDisabled(Level.INFO_INT)) {
            return;
        }
        if (Level.INFO.isGreaterOrEqual(this.getEffectiveLevel())) {
            forcedLog(FQCN, Level.INFO, message, null);
        }
    }

    public void info(Object message, Throwable t) {
        if (getRepository().isDisabled(Level.INFO_INT)) {
            return;
        }
        if (Level.INFO.isGreaterOrEqual(this.getEffectiveLevel())) {
            forcedLog(FQCN, Level.INFO, message, t);
        }
    }

    public boolean isAttached(Appender appender) {
        if (appender == null || appenderPipeline == null) {
            return false;
        } else {
            return appenderPipeline.isAttached(appender);
        }
    }

    public boolean isDebugEnabled() {
        if (getRepository().isDisabled(Level.DEBUG_INT)) {
            return false;
        }
        return Level.DEBUG.isGreaterOrEqual(this.getEffectiveLevel());
    }

    public boolean isErrorEnabled() {
        if (getRepository().isDisabled(Level.ERROR_INT)) {
            return false;
        }
        return Level.ERROR.isGreaterOrEqual(this.getEffectiveLevel());
    }

    public boolean isWarnEnabled() {
        if (getRepository().isDisabled(Level.WARN_INT)) {
            return false;
        }
        return Level.WARN.isGreaterOrEqual(this.getEffectiveLevel());
    }

    public boolean isEnabledFor(Level level) {
        if (getRepository().isDisabled(level.level)) {
            return false;
        }
        return level.isGreaterOrEqual(this.getEffectiveLevel());
    }

    public boolean isInfoEnabled() {
        if (getRepository().isDisabled(Level.INFO_INT)) {
            return false;
        }
        return Level.INFO.isGreaterOrEqual(this.getEffectiveLevel());
    }


    public void log(Level priority, Object message, Throwable t) {
        if (getRepository().isDisabled(priority.level)) {
            return;
        }
        if (priority.isGreaterOrEqual(this.getEffectiveLevel())) {
            forcedLog(FQCN, priority, message, t);
        }
    }


    public void log(Level level, Object message) {
        if (getRepository().isDisabled(level.level)) {
            return;
        }
        if (level.isGreaterOrEqual(this.getEffectiveLevel())) {
            forcedLog(FQCN, level, message, null);
        }
    }

    public void log(String callerFQCN, Level level, Object message, Throwable t) {
        if (getRepository().isDisabled(level.level)) {
            return;
        }
        if (level.isGreaterOrEqual(this.getEffectiveLevel())) {
            forcedLog(callerFQCN, level, message, t);
        }
    }

    synchronized public void removeAllAppenders() {
        if (appenderPipeline != null) {
            Vector appenders = new Vector();
            for (Enumeration iter = appenderPipeline.getAllAppenders(); iter != null && iter.hasMoreElements(); ) {
                appenders.add(iter.nextElement());
            }
            appenderPipeline.removeAllAppenders();
            appenderPipeline = null;
        }
    }

    synchronized public void removeAppender(Appender appender) {
        if (appender == null || appenderPipeline == null) {
            return;
        }
        appenderPipeline.removeAppender(appender);
    }

    synchronized public void removeAppender(String name) {
        if (name == null || appenderPipeline == null) {
            return;
        }
        appenderPipeline.removeAppender(name);
    }

    public void setAdditivity(boolean additive) {
        this.additive = additive;
    }

    public void setLevel(Level level) {
        this.level = level;
    }

    public void warn(Object message) {
        if (getRepository().isDisabled(Level.WARN_INT)) {
            return;
        }

        if (Level.WARN.isGreaterOrEqual(this.getEffectiveLevel())) {
            forcedLog(FQCN, Level.WARN, message, null);
        }
    }

    public void warn(Object message, Throwable t) {
        if (getRepository().isDisabled(Level.WARN_INT)) {
            return;
        }
        if (Level.WARN.isGreaterOrEqual(this.getEffectiveLevel())) {
            forcedLog(FQCN, Level.WARN, message, t);
        }
    }


    public void trace(Object message) {
        if (getRepository().isDisabled(Level.TRACE_INT)) {
            return;
        }

        if (Level.TRACE.isGreaterOrEqual(this.getEffectiveLevel())) {
            forcedLog(FQCN, Level.TRACE, message, null);
        }
    }

    public void trace(Object message, Throwable t) {
        if (getRepository().isDisabled(Level.TRACE_INT)) {
            return;
        }

        if (Level.TRACE.isGreaterOrEqual(this.getEffectiveLevel())) {
            forcedLog(FQCN, Level.TRACE, message, t);
        }
    }

    public boolean isTraceEnabled() {
        if (getRepository().isDisabled(Level.TRACE_INT)) {
            return false;
        }
        return Level.TRACE.isGreaterOrEqual(this.getEffectiveLevel());
    }

    public interface LoggerRepository {

        boolean isDisabled(int level);

        void setThreshold(Level level);

        void emitNoAppenderWarning(Logger cat);

        Level getThreshold();

        Logger getLogger(String name);

        Logger getRootLogger();

        Logger exists(String name);

        void shutdown();

        Enumeration getCurrentLoggers();
    }

    public static class ProvisionNode extends Vector {

        ProvisionNode(Logger logger) {
            super();
            this.addElement(logger);
        }
    }

    public static class DefaultLoggerRepository implements LoggerRepository {

        Hashtable ht;
        Logger root;

        int thresholdInt;
        Level threshold;

        boolean emittedNoAppenderWarning = false;

        public DefaultLoggerRepository(Logger root) {
            ht = new Hashtable();
            this.root = root;
            setThreshold(Level.ALL);
        }

        public void clear() {
            ht.clear();
        }

        public void emitNoAppenderWarning(Logger cat) {
            if (!this.emittedNoAppenderWarning) {
                SysLogger.warn("No appenders could be found for logger (" + cat.getName() + ").");
                SysLogger.warn("Please initialize the logger system properly.");
                this.emittedNoAppenderWarning = true;
            }
        }

        public Logger exists(String name) {
            Object o = ht.get(new CategoryKey(name));
            if (o instanceof Logger) {
                return (Logger) o;
            } else {
                return null;
            }
        }

        public void setThreshold(String levelStr) {
            Level l = Level.toLevel(levelStr, null);
            if (l != null) {
                setThreshold(l);
            } else {
                SysLogger.warn("Could not convert [" + levelStr + "] to Level.");
            }
        }

        public void setThreshold(Level l) {
            if (l != null) {
                thresholdInt = l.level;
                threshold = l;
            }
        }

        public Level getThreshold() {
            return threshold;
        }


        public Logger getLogger(String name) {
            CategoryKey key = new CategoryKey(name);
            Logger logger;

            synchronized (ht) {
                Object o = ht.get(key);
                if (o == null) {
                    logger = makeNewLoggerInstance(name);
                    ht.put(key, logger);
                    updateParents(logger);
                    return logger;
                } else if (o instanceof Logger) {
                    return (Logger) o;
                } else if (o instanceof ProvisionNode) {
                    logger = makeNewLoggerInstance(name);
                    ht.put(key, logger);
                    updateChildren((ProvisionNode) o, logger);
                    updateParents(logger);
                    return logger;
                } else {
                    return null;
                }
            }
        }

        public Logger makeNewLoggerInstance(String name) {
            return new Logger(name);
        }

        public Enumeration getCurrentLoggers() {
            Vector v = new Vector(ht.size());

            Enumeration elems = ht.elements();
            while (elems.hasMoreElements()) {
                Object o = elems.nextElement();
                if (o instanceof Logger) {
                    v.addElement(o);
                }
            }
            return v.elements();
        }

        public Enumeration getCurrentCategories() {
            return getCurrentLoggers();
        }


        public Logger getRootLogger() {
            return root;
        }

        public boolean isDisabled(int level) {
            return thresholdInt > level;
        }


        public void shutdown() {
            Logger root = getRootLogger();
            root.closeNestedAppenders();

            synchronized (ht) {
                Enumeration cats = this.getCurrentLoggers();
                while (cats.hasMoreElements()) {
                    Logger c = (Logger) cats.nextElement();
                    c.closeNestedAppenders();
                }
                root.removeAllAppenders();
            }
        }


        final private void updateParents(Logger cat) {
            String name = cat.name;
            int length = name.length();
            boolean parentFound = false;

            for (int i = name.lastIndexOf('.', length - 1); i >= 0;
                 i = name.lastIndexOf('.', i - 1)) {
                String substr = name.substring(0, i);

                CategoryKey key = new CategoryKey(substr);
                Object o = ht.get(key);
                if (o == null) {
                    ProvisionNode pn = new ProvisionNode(cat);
                    ht.put(key, pn);
                } else if (o instanceof Logger) {
                    parentFound = true;
                    cat.parent = (Logger) o;
                    break;
                } else if (o instanceof ProvisionNode) {
                    ((ProvisionNode) o).addElement(cat);
                } else {
                    Exception e = new IllegalStateException("unexpected object type " + o.getClass() + " in ht.");
                    e.printStackTrace();
                }
            }
            if (!parentFound) {
                cat.parent = root;
            }
        }

        final private void updateChildren(ProvisionNode pn, Logger logger) {
            final int last = pn.size();

            for (int i = 0; i < last; i++) {
                Logger l = (Logger) pn.elementAt(i);
                if (!l.parent.name.startsWith(logger.name)) {
                    logger.parent = l.parent;
                    l.parent = logger;
                }
            }
        }

        private class CategoryKey {

            String name;
            int hashCache;

            CategoryKey(String name) {
                this.name = name;
                hashCache = name.hashCode();
            }

            final public int hashCode() {
                return hashCache;
            }

            final public boolean equals(Object rArg) {
                if (this == rArg) {
                    return true;
                }

                if (rArg != null && CategoryKey.class == rArg.getClass()) {
                    return name.equals(((CategoryKey) rArg).name);
                } else {
                    return false;
                }
            }
        }

    }

    public static class RootLogger extends Logger {

        public RootLogger(Level level) {
            super("root");
            setLevel(level);
        }
    }
}
