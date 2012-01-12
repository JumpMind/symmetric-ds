package org.jumpmind.log;

abstract public class Log {

    protected String category;

    public abstract void log(LogLevel level, Throwable error, String msg, Object... params);

    public void log(LogLevel level, String msg, Object... params) {
        log(level, null, msg, params);
    }

    public void log(LogLevel level, Throwable error) {
        log(level, error, null);
    }

    public void debug(String msg, Object... params) {
        log(LogLevel.DEBUG, msg, params);
    }

    public void info(String msg, Object... params) {
        log(LogLevel.INFO, msg, params);
    }

    public void warn(String msg, Object... params) {
        log(LogLevel.WARN, msg, params);
    }
    
    public void warn(Throwable ex) {
        log(LogLevel.WARN, ex);
    }
    
    public void warn(String msg, Throwable ex, Object... params) {
        log(LogLevel.WARN, ex, msg, params);
    }
    
    public void error(String msg, Object... params) {
        log(LogLevel.ERROR, msg, params);
    }
    
    public void error(String msg, Throwable ex, Object... params) {
        log(LogLevel.ERROR, ex, msg, params);
    }

    public void error(Throwable ex) {
        log(LogLevel.ERROR, ex);
    }
    
    public String getCategory() {
        return category;
    }

    public abstract boolean isDebugEnabled();

    protected void initialize(String category) {
        this.category = category;
    }

}
