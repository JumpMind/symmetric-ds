package org.jumpmind.symmetric.core.common;

import java.util.logging.Level;
import java.util.logging.Logger;

public class DefaultLog extends Log {

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
    public void debug(String msg) {
        log(LogLevel.DEBUG, msg);
    }

    @Override
    public boolean isDebugEnabled() {
        return logger.isLoggable(Level.FINE);
    }

    public void log(LogLevel level, Throwable error, String msg, Object... params) {
        Level loggerLevel = Level.SEVERE;

        switch (level) {
        case DEBUG:
            loggerLevel = Level.FINE;
            break;
        case INFO:
            loggerLevel = Level.INFO;
            break;
        case WARN:
            loggerLevel = Level.WARNING;
            break;
        }

        if (logger.isLoggable(loggerLevel)) {
            if (error != null && params == null) {
                logger.log(loggerLevel, msg, error);
            } else if (error != null && msg != null && params != null) {
                logger.log(loggerLevel, msg, params);
                logger.log(loggerLevel, error.getMessage(), error);
            } else if (msg != null) {
                logger.log(loggerLevel, msg, params);
            }
        }

    }
}
