package org.jumpmind.symmetric.core.common;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class Log4jLog extends Log {

    Logger logger;

    @Override
    protected void initialize(Class<?> clazz) {
        super.initialize(clazz);
        logger = Logger.getLogger(clazz.getName());
    }

    public void log(LogLevel level, String msg, Object... params) {
        log(level, null, msg, params);
    }

    public void log(LogLevel level, Throwable error) {
        log(level, error, null);
    }

    @Override
    public void error(Throwable ex) {
        log(LogLevel.ERROR, ex);
    }

    @Override
    public void debug(String msg, Object... params) {
        log(LogLevel.DEBUG, msg, params);
    }

    @Override
    public boolean isDebugEnabled() {
        return logger.isDebugEnabled();
    }

    public void log(LogLevel level, Throwable error, String msg, Object... params) {
        Level loggerLevel = Level.FATAL;

        switch (level) {
        case DEBUG:
            loggerLevel = Level.DEBUG;
            break;
        case INFO:
            loggerLevel = Level.INFO;
            break;
        case WARN:
            loggerLevel = Level.WARN;
            break;
        }

        if (logger.isEnabledFor(loggerLevel)) {
            if (error != null && params == null) {
                logger.log(loggerLevel, msg, error);
            } else if (error != null && msg != null && params != null) {
                logger.log(loggerLevel, String.format(msg, params));
                logger.log(loggerLevel, error.getMessage(), error);
            } else if (msg != null) {
                logger.log(loggerLevel, String.format(msg, params));
            }
        }

    }
}
