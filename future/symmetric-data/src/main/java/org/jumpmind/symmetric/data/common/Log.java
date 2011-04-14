package org.jumpmind.symmetric.data.common;


abstract public class Log {

    Class<?> clazz;
    
    public abstract void log (LogLevel level, String msg, Object ... params);
    
    public abstract void log (LogLevel level, Throwable error, String msg, Object ... params);
    
    public abstract void log(LogLevel level, Throwable error);
    
    protected void setClass(Class<?> clazz) {
        this.clazz = clazz;
    }

}
