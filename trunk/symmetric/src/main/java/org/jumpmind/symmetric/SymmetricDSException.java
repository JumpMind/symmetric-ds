package org.jumpmind.symmetric;

import org.jumpmind.symmetric.common.Message;

public class SymmetricDSException extends RuntimeException {

    /**
     * 
     */
    private static final long serialVersionUID = -3111453874504638368L;

    public SymmetricDSException() {
        super();
    }

    public SymmetricDSException(Throwable cause) {
        super(cause);
    }

    public SymmetricDSException(String messageKey) {
        super(Message.get(messageKey));
    }

    public SymmetricDSException(String messageKey, Object... args) {
        super(Message.get(messageKey, args));
    }

    public SymmetricDSException(String messageKey, Throwable cause) {
        super(Message.get(messageKey), cause);
    }

    public SymmetricDSException(String messageKey, Throwable cause, Object... args) {
        super(Message.get(messageKey, args), cause);
    }

}
