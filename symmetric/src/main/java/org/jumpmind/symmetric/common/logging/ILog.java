package org.jumpmind.symmetric.common.logging;

import java.util.Locale;

public interface ILog {

    public abstract String getMessage(String key);

    public abstract String getMessage(String key, Object... args);

    public abstract Locale getLocale();

    public abstract void setLocale(Locale locale);

    public abstract String getBundleName();

    public abstract void setBundleName(String bundleName);

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

    public abstract void error(String messageKey);

    public abstract void error(String messageKey, Throwable t);

    public abstract void error(String messageKey, Object... args);

    public abstract void error(String messageKey, Throwable t, Object... args);

    public abstract void error(Throwable t);

}