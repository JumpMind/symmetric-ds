package org.jumpmind.symmetric.core.common;

public class ConsoleLog extends Log {

    @Override
    public boolean isDebugEnabled() {
        return getLogLevel().contains("debug");
    }

    public boolean isInfoEnabled() {
        return isDebugEnabled() || getLogLevel().contains("info");
    }

    public boolean isWarnEnabled() {
        return isInfoEnabled() || getLogLevel().contains("warn");
    }

    protected String getLogLevel() {
        return System.getProperty("log", "info").toLowerCase();
    }

    protected boolean isLogLevelEnabled(LogLevel level) {
        switch (level) {
        case DEBUG:
            return isDebugEnabled();
        case INFO:
            return isInfoEnabled() || isDebugEnabled();
        case WARN:
            return isWarnEnabled() || isInfoEnabled() || isDebugEnabled();
        case ERROR:
            return true;
        default:
            return false;
        }
    }

    public void log(LogLevel level, Throwable error, String msg, Object... params) {
        if (isLogLevelEnabled(level)) {
            if (StringUtils.isNotBlank(msg) && params != null && params.length > 0) {
                msg = String.format(msg, params);
            }

            if (StringUtils.isNotBlank(msg)) {
                if (msg.endsWith("\r")) {
                    System.out.print(msg);
                } else {
                    System.out.println(msg);
                }
            }

            if (error != null) {
                error.printStackTrace();
            }
        }

    }
}
