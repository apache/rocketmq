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

        for (Logger logger = this; logger != null; logger = logger.parent) {
            synchronized (logger) {
                if (logger.appenderPipeline != null) {
                    writes += logger.appenderPipeline.appendLoopOnAppenders(event);
                }
                if (!logger.additive) {
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
        return appender != null && appenderPipeline != null && appenderPipeline.isAttached(appender);
    }

    synchronized public void removeAllAppenders() {
        if (appenderPipeline != null) {
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

    public interface LoggerRepository {

        boolean isDisabled(int level);

        void setLogLevel(Level level);

        void emitNoAppenderWarning(Logger cat);

        Level getLogLevel();

        Logger getLogger(String name);

        Logger getRootLogger();

        Logger exists(String name);

        void shutdown();

        Enumeration getCurrentLoggers();
    }

    public static class ProvisionNode extends Vector<Logger> {

        ProvisionNode(Logger logger) {
            super();
            addElement(logger);
        }
    }

    public static class DefaultLoggerRepository implements LoggerRepository {

        final Hashtable<CategoryKey,Object> ht = new Hashtable<CategoryKey,Object>();
        Logger root;

        int logLevelInt;
        Level logLevel;

        boolean emittedNoAppenderWarning = false;

        public DefaultLoggerRepository(Logger root) {
            this.root = root;
            setLogLevel(Level.ALL);
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

        public void setLogLevel(Level l) {
            if (l != null) {
                logLevelInt = l.level;
                logLevel = l;
            }
        }

        public Level getLogLevel() {
            return logLevel;
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
            Vector<Logger> loggers = new Vector<Logger>(ht.size());

            Enumeration elems = ht.elements();
            while (elems.hasMoreElements()) {
                Object o = elems.nextElement();
                if (o instanceof Logger) {
                    Logger logger = (Logger)o;
                    loggers.addElement(logger);
                }
            }
            return loggers.elements();
        }


        public Logger getRootLogger() {
            return root;
        }

        public boolean isDisabled(int level) {
            return logLevelInt > level;
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


        private void updateParents(Logger cat) {
            String name = cat.name;
            int length = name.length();
            boolean parentFound = false;

            for (int i = name.lastIndexOf('.', length - 1); i >= 0;
                 i = name.lastIndexOf('.', i - 1)) {
                String substr = name.substring(0, i);

                CategoryKey key = new CategoryKey(substr);
                Object o = ht.get(key);
                if (o == null) {
                    ht.put(key, new ProvisionNode(cat));
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

        private void updateChildren(ProvisionNode pn, Logger logger) {
            final int last = pn.size();

            for (int i = 0; i < last; i++) {
                Logger l = pn.elementAt(i);
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

            final public boolean equals(Object o) {
                if (this == o) {
                    return true;
                }

                if (o != null && o instanceof CategoryKey) {
                    CategoryKey cc = (CategoryKey) o;
                    return name.equals(cc.name);
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
