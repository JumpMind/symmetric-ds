package org.jumpmind.symmetric.core.common;

abstract public class Log {

    Class<?> clazz;

    public abstract void log(LogLevel level, String msg, Object... params);

    public abstract void log(LogLevel level, Throwable error, String msg, Object... params);

    public abstract void log(LogLevel level, Throwable error);

    public void debug(String msg, Object... params) {
        log(LogLevel.DEBUG, msg, params);
    }

    public void info(String msg, Object... params) {
        log(LogLevel.INFO, msg, params);
    }
    
    public void error(Throwable ex) {
        log(LogLevel.ERROR, ex);
    }

    public abstract boolean isDebugEnabled();

    protected void initialize(Class<?> clazz) {
        this.clazz = clazz;
    }

}
