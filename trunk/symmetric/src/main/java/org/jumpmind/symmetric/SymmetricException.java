package org.jumpmind.symmetric;

import org.jumpmind.symmetric.common.Message;

public class SymmetricException extends RuntimeException {

    /**
     * 
     */
    private static final long serialVersionUID = -3111453874504638368L;

    public SymmetricException() {
        super();
    }

    public SymmetricException(Throwable cause) {
        super(cause);
    }

    public SymmetricException(String messageKey) {
        super(Message.get(messageKey));
    }

    public SymmetricException(String messageKey, Object... args) {
        super(Message.get(messageKey, args));
    }

    public SymmetricException(String messageKey, Throwable cause) {
        super(Message.get(messageKey), cause);
    }

    public SymmetricException(String messageKey, Throwable cause, Object... args) {
        super(Message.get(messageKey, args), cause);
    }

}
