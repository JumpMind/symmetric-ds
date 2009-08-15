package org.jumpmind.symmetric.common.logging;

import java.util.Locale;
import java.util.MissingResourceException;
import java.util.ResourceBundle;
import java.util.UnknownFormatConversionException;

public class Log {

    private org.apache.commons.logging.Log logger = null;

    private Locale locale = null;

    private String bundleName = null;

    private ResourceBundle bundle = null;
    private static String MESSAGE_KEY = "MessageKey: ";
    private static String DEFAULT_BUNDLE_NAME = "logMessages";

    Log(org.apache.commons.logging.Log logger) {
        this(logger, DEFAULT_BUNDLE_NAME, Locale.getDefault());
    }

    Log(org.apache.commons.logging.Log logger, String bundleName) {
        this(logger, bundleName, Locale.getDefault());
    }

    Log(org.apache.commons.logging.Log logger, String bundleName, Locale locale) {
        this.logger = logger;
        this.bundleName = bundleName;
        setLocale(locale);
    }

    public String getMessage(String key) {
        return getMessage(key, (Object) null);
    }

    public String getMessage(String key, Object... args) {
        if (bundle != null) {
            try {
                if (args != null) {
                    return String.format(locale, bundle.getString(key), args);
                } else {
                    return String.format(locale, bundle.getString(key));
                }
            } catch (MissingResourceException mre) {
                return MESSAGE_KEY + key + ((args != null) ? args.toString() : "");
            } catch (UnknownFormatConversionException ufce) {
                logger.error(MESSAGE_KEY + key + " has a bad format type: " + bundle.getString(key));
                return MESSAGE_KEY + key + ((args != null) ? args.toString() : "");
            }
        } else {
            return MESSAGE_KEY + key + ((args != null) ? args.toString() : "");
        }
    }

    public Locale getLocale() {
        return locale;
    }

    public void setLocale(Locale locale) {
        this.locale = locale;
        try {
            this.bundle = ResourceBundle.getBundle(bundleName, locale);
        } catch (MissingResourceException e) {
            this.bundle = null;
            logger.error("Unable to locate resource bundle: " + bundleName);
        }
    }

    public String getBundleName() {
        return bundleName;
    }

    public void setBundleName(String bundleName) {
        this.bundleName = bundleName;
        this.bundle = ResourceBundle.getBundle(bundleName, locale);
    }

    // Debug

    public void debug(String messageKey) {
        if (logger.isDebugEnabled()) {
            logger.debug(getMessage(messageKey));
        }
    }

    public void debug(String messageKey, Throwable t) {
        if (logger.isDebugEnabled()) {
            logger.debug(getMessage(messageKey), t);
        }
    }

    public void debug(String messageKey, Object... args) {
        if (logger.isDebugEnabled()) {
            logger.debug(getMessage(messageKey, args));
        }
    }

    public void debug(String messageKey, Throwable t, Object... args) {
        if (logger.isDebugEnabled()) {
            logger.debug(getMessage(messageKey, args), t);
        }
    }

    // info

    public void info(String messageKey) {
        if (logger.isInfoEnabled()) {
            logger.info(getMessage(messageKey));
        }
    }

    public void info(String messageKey, Throwable t) {
        if (logger.isInfoEnabled()) {
            logger.info(getMessage(messageKey), t);
        }
    }

    public void info(String messageKey, Object... args) {
        if (logger.isInfoEnabled()) {
            logger.info(getMessage(messageKey, args));
        }
    }

    public void info(String messageKey, Throwable t, Object... args) {
        if (logger.isInfoEnabled()) {
            logger.info(getMessage(messageKey, args), t);
        }
    }

    // warn

    public void warn(String messageKey) {
        if (logger.isWarnEnabled()) {
            logger.info(getMessage(messageKey));
        }
    }

    public void warn(String messageKey, Throwable t) {
        if (logger.isWarnEnabled()) {
            logger.info(getMessage(messageKey), t);
        }
    }

    public void warn(String messageKey, Object... args) {
        if (logger.isWarnEnabled()) {
            logger.info(getMessage(messageKey, args));
        }
    }

    public void warn(String messageKey, Throwable t, Object... args) {
        if (logger.isWarnEnabled()) {
            logger.info(getMessage(messageKey, args), t);
        }
    }

    // error

    public void error(String messageKey) {
        if (logger.isErrorEnabled()) {
            logger.info(getMessage(messageKey));
        }
    }

    public void error(String messageKey, Throwable t) {
        if (logger.isErrorEnabled()) {
            logger.info(getMessage(messageKey), t);
        }
    }

    public void error(String messageKey, Object... args) {
        if (logger.isErrorEnabled()) {
            logger.info(getMessage(messageKey, args));
        }
    }

    public void error(String messageKey, Throwable t, Object... args) {
        if (logger.isErrorEnabled()) {
            logger.info(getMessage(messageKey, args), t);
        }
    }

}
