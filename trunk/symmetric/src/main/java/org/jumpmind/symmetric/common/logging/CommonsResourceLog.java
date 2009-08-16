package org.jumpmind.symmetric.common.logging;

import org.jumpmind.symmetric.common.Message;

public class CommonsResourceLog implements ILog {

    private org.apache.commons.logging.Log commonsLog = null;

    CommonsResourceLog(org.apache.commons.logging.Log log) {
        this.commonsLog = log;
    }

    // Debug

    public boolean isDebugEnabled() {
        return commonsLog.isDebugEnabled();
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.jumpmind.symmetric.common.logging.ILog#debug(java.lang.String)
     */
    public void debug(String messageKey) {
        if (commonsLog.isDebugEnabled()) {
            commonsLog.debug(Message.get(messageKey));
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.jumpmind.symmetric.common.logging.ILog#debug(java.lang.String,
     * java.lang.Throwable)
     */
    public void debug(String messageKey, Throwable t) {
        if (commonsLog.isDebugEnabled()) {
            commonsLog.debug(Message.get(messageKey), t);
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.jumpmind.symmetric.common.logging.ILog#debug(java.lang.String,
     * java.lang.Object)
     */
    public void debug(String messageKey, Object... args) {
        if (commonsLog.isDebugEnabled()) {
            commonsLog.debug(Message.get(messageKey, args));
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.jumpmind.symmetric.common.logging.ILog#debug(java.lang.String,
     * java.lang.Throwable, java.lang.Object)
     */
    public void debug(String messageKey, Throwable t, Object... args) {
        if (commonsLog.isDebugEnabled()) {
            commonsLog.debug(Message.get(messageKey, args), t);
        }
    }

    // info

    public boolean isInfoEnabled() {
        return commonsLog.isInfoEnabled();
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.jumpmind.symmetric.common.logging.ILog#info(java.lang.String)
     */
    public void info(String messageKey) {
        if (commonsLog.isInfoEnabled()) {
            commonsLog.info(Message.get(messageKey));
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.jumpmind.symmetric.common.logging.ILog#info(java.lang.String,
     * java.lang.Throwable)
     */
    public void info(String messageKey, Throwable t) {
        if (commonsLog.isInfoEnabled()) {
            commonsLog.info(Message.get(messageKey), t);
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.jumpmind.symmetric.common.logging.ILog#info(java.lang.String,
     * java.lang.Object)
     */
    public void info(String messageKey, Object... args) {
        if (commonsLog.isInfoEnabled()) {
            commonsLog.info(Message.get(messageKey, args));
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.jumpmind.symmetric.common.logging.ILog#info(java.lang.String,
     * java.lang.Throwable, java.lang.Object)
     */
    public void info(String messageKey, Throwable t, Object... args) {
        if (commonsLog.isInfoEnabled()) {
            commonsLog.info(Message.get(messageKey, args), t);
        }
    }

    // warn

    public boolean isWarnEnabled() {
        return commonsLog.isWarnEnabled();
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.jumpmind.symmetric.common.logging.ILog#warn(java.lang.String)
     */
    public void warn(String messageKey) {
        if (commonsLog.isWarnEnabled()) {
            commonsLog.warn(Message.get(messageKey));
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.jumpmind.symmetric.common.logging.ILog#warn(java.lang.String,
     * java.lang.Throwable)
     */
    public void warn(String messageKey, Throwable t) {
        if (commonsLog.isWarnEnabled()) {
            commonsLog.warn(Message.get(messageKey), t);
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.jumpmind.symmetric.common.logging.ILog#warn(java.lang.String,
     * java.lang.Object)
     */
    public void warn(String messageKey, Object... args) {
        if (commonsLog.isWarnEnabled()) {
            commonsLog.warn(Message.get(messageKey, args));
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * org.jumpmind.symmetric.common.logging.ILog#fatal(java.lang.Throwable)
     */
    public void warn(Throwable t) {
        if (commonsLog.isWarnEnabled()) {
            commonsLog.warn(t, t);
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.jumpmind.symmetric.common.logging.ILog#warn(java.lang.String,
     * java.lang.Throwable, java.lang.Object)
     */
    public void warn(String messageKey, Throwable t, Object... args) {
        if (commonsLog.isWarnEnabled()) {
            commonsLog.warn(Message.get(messageKey, args), t);
        }
    }

    // error

    public boolean isErrorEnabled() {
        return commonsLog.isErrorEnabled();
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.jumpmind.symmetric.common.logging.ILog#error(java.lang.String)
     */
    public void error(String messageKey) {
        if (commonsLog.isErrorEnabled()) {
            commonsLog.error(Message.get(messageKey));
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.jumpmind.symmetric.common.logging.ILog#error(java.lang.String,
     * java.lang.Throwable)
     */
    public void error(String messageKey, Throwable t) {
        if (commonsLog.isErrorEnabled()) {
            commonsLog.error(Message.get(messageKey), t);
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.jumpmind.symmetric.common.logging.ILog#error(java.lang.String,
     * java.lang.Object)
     */
    public void error(String messageKey, Object... args) {
        if (commonsLog.isErrorEnabled()) {
            commonsLog.error(Message.get(messageKey, args));
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.jumpmind.symmetric.common.logging.ILog#error(java.lang.String,
     * java.lang.Throwable, java.lang.Object)
     */
    public void error(String messageKey, Throwable t, Object... args) {
        if (commonsLog.isErrorEnabled()) {
            commonsLog.error(Message.get(messageKey, args), t);
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * org.jumpmind.symmetric.common.logging.ILog#error(java.lang.Throwable)
     */
    public void error(Throwable t) {
        if (commonsLog.isErrorEnabled()) {
            commonsLog.error(t, t);
        }
    }

    // fatal

    /*
     * (non-Javadoc)
     * 
     * @see org.jumpmind.symmetric.common.logging.ILog#fatal(java.lang.String)
     */
    public void fatal(String messageKey) {
        if (commonsLog.isFatalEnabled()) {
            commonsLog.fatal(Message.get(messageKey));
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.jumpmind.symmetric.common.logging.ILog#fatal(java.lang.String,
     * java.lang.Throwable)
     */
    public void fatal(String messageKey, Throwable t) {
        if (commonsLog.isFatalEnabled()) {
            commonsLog.fatal(Message.get(messageKey), t);
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.jumpmind.symmetric.common.logging.ILog#fatal(java.lang.String,
     * java.lang.Object)
     */
    public void fatal(String messageKey, Object... args) {
        if (commonsLog.isFatalEnabled()) {
            commonsLog.fatal(Message.get(messageKey, args));
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.jumpmind.symmetric.common.logging.ILog#fatal(java.lang.String,
     * java.lang.Throwable, java.lang.Object)
     */
    public void fatal(String messageKey, Throwable t, Object... args) {
        if (commonsLog.isFatalEnabled()) {
            commonsLog.fatal(Message.get(messageKey, args), t);
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * org.jumpmind.symmetric.common.logging.ILog#fatal(java.lang.Throwable)
     */
    public void fatal(Throwable t) {
        if (commonsLog.isFatalEnabled()) {
            commonsLog.fatal(t, t);
        }
    }

}
