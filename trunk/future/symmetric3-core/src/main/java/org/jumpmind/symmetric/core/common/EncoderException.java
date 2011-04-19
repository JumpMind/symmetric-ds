package org.jumpmind.symmetric.core.common;

public class EncoderException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public EncoderException() {

    }

    public EncoderException(String message, Throwable cause) {
        super(message, cause);

    }

    public EncoderException(String message) {
        super(message);

    }

    public EncoderException(Throwable cause) {
        super(cause);

    }

}
