package org.jumpmind.symmetric.core.common;


abstract public class Log {

    Class<?> clazz;
    
    public abstract void log (LogLevel level, String msg, Object ... params);
    
    public abstract void log (LogLevel level, Throwable error, String msg, Object ... params);
    
    public abstract void log(LogLevel level, Throwable error);
    
    public abstract void debug(String msg);
    
    public abstract boolean isDebugEnabled();
    
    public abstract void error(Throwable ex);
    
    protected void initialize(Class<?> clazz) {
        this.clazz = clazz;
    }

}
