package org.jumpmind.symmetric.common.logging;

import java.util.Locale;

public interface ILog {

    public abstract void debug(String messageKey);

    public abstract void debug(String messageKey, Throwable t);

    public abstract void debug(String messageKey, Object... args);

    public abstract void debug(String messageKey, Throwable t, Object... args);

    public abstract void info(String messageKey);

    public abstract void info(String messageKey, Throwable t);

    public abstract void info(String messageKey, Object... args);

    public abstract void info(String messageKey, Throwable t, Object... args);

    public abstract void warn(String messageKey);

    public abstract void warn(String messageKey, Throwable t);

    public abstract void warn(String messageKey, Object... args);

    public abstract void warn(String messageKey, Throwable t, Object... args);
    
    public abstract void warn(Throwable t);

    public abstract void error(String messageKey);

    public abstract void error(String messageKey, Throwable t);

    public abstract void error(String messageKey, Object... args);

    public abstract void error(String messageKey, Throwable t, Object... args);

    public abstract void error(Throwable t);

    public abstract void fatal(String messageKey);

    public abstract void fatal(String messageKey, Throwable t);

    public abstract void fatal(String messageKey, Object... args);

    public abstract void fatal(String messageKey, Throwable t, Object... args);

    public abstract void fatal(Throwable t);

}