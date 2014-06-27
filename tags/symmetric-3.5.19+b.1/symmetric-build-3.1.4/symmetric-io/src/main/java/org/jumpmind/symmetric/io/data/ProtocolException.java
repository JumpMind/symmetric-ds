package org.jumpmind.symmetric.io.data;

public class ProtocolException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public ProtocolException() {
        super();
    }

    public ProtocolException(String message, Throwable cause) {
        super(message, cause);    }

    public ProtocolException(String message, Object ... args) {
        super(String.format(message, args));
    }

    public ProtocolException(Throwable cause) {
        super(cause);
    }

}
