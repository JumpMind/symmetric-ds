package org.jumpmind.symmetric.common.logging;

import java.util.Locale;
import java.util.MissingResourceException;
import java.util.ResourceBundle;
import java.util.UnknownFormatConversionException;

public class CommonsResourceLog implements ILog {

    private org.apache.commons.logging.Log logger = null;

    private Locale locale = null;

    private String bundleName = null;

    private ResourceBundle bundle = null;
    private static String MESSAGE_KEY = "MessageKey: ";
    private static String DEFAULT_BUNDLE_NAME = "messages";

    CommonsResourceLog(org.apache.commons.logging.Log logger) {
        this(logger, DEFAULT_BUNDLE_NAME, Locale.getDefault());
    }

    CommonsResourceLog(org.apache.commons.logging.Log logger, String bundleName) {
        this(logger, bundleName, Locale.getDefault());
    }

    CommonsResourceLog(org.apache.commons.logging.Log logger, String bundleName, Locale locale) {
        this.logger = logger;
        this.bundleName = bundleName;
        setLocale(locale);
    }

    /* (non-Javadoc)
     * @see org.jumpmind.symmetric.common.logging.ILog#getMessage(java.lang.String)
     */
    public String getMessage(String key) {
        return getMessage(key, (Object) null);
    }

    /* (non-Javadoc)
     * @see org.jumpmind.symmetric.common.logging.ILog#getMessage(java.lang.String, java.lang.Object)
     */
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

    /* (non-Javadoc)
     * @see org.jumpmind.symmetric.common.logging.ILog#getLocale()
     */
    public Locale getLocale() {
        return locale;
    }

    /* (non-Javadoc)
     * @see org.jumpmind.symmetric.common.logging.ILog#setLocale(java.util.Locale)
     */
    public void setLocale(Locale locale) {
        this.locale = locale;
        try {
            this.bundle = ResourceBundle.getBundle(bundleName, locale);
        } catch (MissingResourceException e) {
            this.bundle = null;
            logger.error("Unable to locate resource bundle: " + bundleName);
        }
    }

    /* (non-Javadoc)
     * @see org.jumpmind.symmetric.common.logging.ILog#getBundleName()
     */
    public String getBundleName() {
        return bundleName;
    }

    /* (non-Javadoc)
     * @see org.jumpmind.symmetric.common.logging.ILog#setBundleName(java.lang.String)
     */
    public void setBundleName(String bundleName) {
        this.bundleName = bundleName;
        this.bundle = ResourceBundle.getBundle(bundleName, locale);
    }

    // Debug

    /* (non-Javadoc)
     * @see org.jumpmind.symmetric.common.logging.ILog#debug(java.lang.String)
     */
    public void debug(String messageKey) {
        if (logger.isDebugEnabled()) {
            logger.debug(getMessage(messageKey));
        }
    }

    /* (non-Javadoc)
     * @see org.jumpmind.symmetric.common.logging.ILog#debug(java.lang.String, java.lang.Throwable)
     */
    public void debug(String messageKey, Throwable t) {
        if (logger.isDebugEnabled()) {
            logger.debug(getMessage(messageKey), t);
        }
    }

    /* (non-Javadoc)
     * @see org.jumpmind.symmetric.common.logging.ILog#debug(java.lang.String, java.lang.Object)
     */
    public void debug(String messageKey, Object... args) {
        if (logger.isDebugEnabled()) {
            logger.debug(getMessage(messageKey, args));
        }
    }

    /* (non-Javadoc)
     * @see org.jumpmind.symmetric.common.logging.ILog#debug(java.lang.String, java.lang.Throwable, java.lang.Object)
     */
    public void debug(String messageKey, Throwable t, Object... args) {
        if (logger.isDebugEnabled()) {
            logger.debug(getMessage(messageKey, args), t);
        }
    }

    // info

    /* (non-Javadoc)
     * @see org.jumpmind.symmetric.common.logging.ILog#info(java.lang.String)
     */
    public void info(String messageKey) {
        if (logger.isInfoEnabled()) {
            logger.info(getMessage(messageKey));
        }
    }

    /* (non-Javadoc)
     * @see org.jumpmind.symmetric.common.logging.ILog#info(java.lang.String, java.lang.Throwable)
     */
    public void info(String messageKey, Throwable t) {
        if (logger.isInfoEnabled()) {
            logger.info(getMessage(messageKey), t);
        }
    }

    /* (non-Javadoc)
     * @see org.jumpmind.symmetric.common.logging.ILog#info(java.lang.String, java.lang.Object)
     */
    public void info(String messageKey, Object... args) {
        if (logger.isInfoEnabled()) {
            logger.info(getMessage(messageKey, args));
        }
    }

    /* (non-Javadoc)
     * @see org.jumpmind.symmetric.common.logging.ILog#info(java.lang.String, java.lang.Throwable, java.lang.Object)
     */
    public void info(String messageKey, Throwable t, Object... args) {
        if (logger.isInfoEnabled()) {
            logger.info(getMessage(messageKey, args), t);
        }
    }

    // warn

    /* (non-Javadoc)
     * @see org.jumpmind.symmetric.common.logging.ILog#warn(java.lang.String)
     */
    public void warn(String messageKey) {
        if (logger.isWarnEnabled()) {
            logger.warn(getMessage(messageKey));
        }
    }

    /* (non-Javadoc)
     * @see org.jumpmind.symmetric.common.logging.ILog#warn(java.lang.String, java.lang.Throwable)
     */
    public void warn(String messageKey, Throwable t) {
        if (logger.isWarnEnabled()) {
            logger.warn(getMessage(messageKey), t);
        }
    }

    /* (non-Javadoc)
     * @see org.jumpmind.symmetric.common.logging.ILog#warn(java.lang.String, java.lang.Object)
     */
    public void warn(String messageKey, Object... args) {
        if (logger.isWarnEnabled()) {
            logger.warn(getMessage(messageKey, args));
        }
    }

    /* (non-Javadoc)
     * @see org.jumpmind.symmetric.common.logging.ILog#warn(java.lang.String, java.lang.Throwable, java.lang.Object)
     */
    public void warn(String messageKey, Throwable t, Object... args) {
        if (logger.isWarnEnabled()) {
            logger.warn(getMessage(messageKey, args), t);
        }
    }

    // error

    /* (non-Javadoc)
     * @see org.jumpmind.symmetric.common.logging.ILog#error(java.lang.String)
     */
    public void error(String messageKey) {
        if (logger.isErrorEnabled()) {
            logger.error(getMessage(messageKey));
        }
    }

    /* (non-Javadoc)
     * @see org.jumpmind.symmetric.common.logging.ILog#error(java.lang.String, java.lang.Throwable)
     */
    public void error(String messageKey, Throwable t) {
        if (logger.isErrorEnabled()) {
            logger.error(getMessage(messageKey), t);
        }
    }

    /* (non-Javadoc)
     * @see org.jumpmind.symmetric.common.logging.ILog#error(java.lang.String, java.lang.Object)
     */
    public void error(String messageKey, Object... args) {
        if (logger.isErrorEnabled()) {
            logger.error(getMessage(messageKey, args));
        }
    }

    /* (non-Javadoc)
     * @see org.jumpmind.symmetric.common.logging.ILog#error(java.lang.String, java.lang.Throwable, java.lang.Object)
     */
    public void error(String messageKey, Throwable t, Object... args) {
        if (logger.isErrorEnabled()) {
            logger.error(getMessage(messageKey, args), t);
        }
    }
    

    /* (non-Javadoc)
     * @see org.jumpmind.symmetric.common.logging.ILog#error(java.lang.Throwable)
     */
    public void error(Throwable t) {
        if (logger.isErrorEnabled()) {
            logger.error(t,t);
        }
    }
    
    // fatal

    /* (non-Javadoc)
     * @see org.jumpmind.symmetric.common.logging.ILog#fatal(java.lang.String)
     */
    public void fatal(String messageKey) {
        if (logger.isFatalEnabled()) {
            logger.fatal(getMessage(messageKey));
        }
    }

    /* (non-Javadoc)
     * @see org.jumpmind.symmetric.common.logging.ILog#fatal(java.lang.String, java.lang.Throwable)
     */
    public void fatal(String messageKey, Throwable t) {
        if (logger.isFatalEnabled()) {
            logger.fatal(getMessage(messageKey), t);
        }
    }

    /* (non-Javadoc)
     * @see org.jumpmind.symmetric.common.logging.ILog#fatal(java.lang.String, java.lang.Object)
     */
    public void fatal(String messageKey, Object... args) {
        if (logger.isFatalEnabled()) {
            logger.fatal(getMessage(messageKey, args));
        }
    }

    /* (non-Javadoc)
     * @see org.jumpmind.symmetric.common.logging.ILog#fatal(java.lang.String, java.lang.Throwable, java.lang.Object)
     */
    public void fatal(String messageKey, Throwable t, Object... args) {
        if (logger.isFatalEnabled()) {
            logger.fatal(getMessage(messageKey, args), t);
        }
    }
    

    /* (non-Javadoc)
     * @see org.jumpmind.symmetric.common.logging.ILog#fatal(java.lang.Throwable)
     */
    public void fatal(Throwable t) {
        if (logger.isFatalEnabled()) {
            logger.fatal(t,t);
        }
    }

}
