package org.jumpmind.symmetric.core.common;

import java.util.logging.Level;
import java.util.logging.Logger;

public class DefaultLog extends Log {

    Logger logger;

    public void log(LogLevel level, String msg, Object... params) {
        log(level, null, msg, params);
    }

    public void log(LogLevel level, Throwable error) {
        log(level, error, null);
    }

    public void log(LogLevel level, Throwable error, String msg, Object... params) {
        if (logger == null) {
            logger = Logger.getLogger(clazz.getName());
        }

        Level loggerLevel = null;

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
        default:
            loggerLevel = Level.SEVERE;
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
