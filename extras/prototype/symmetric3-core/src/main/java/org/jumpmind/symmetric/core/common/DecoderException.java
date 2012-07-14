package org.jumpmind.symmetric.core.common;

public class DecoderException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public DecoderException() {

    }

    public DecoderException(String message, Throwable cause) {
        super(message, cause);

    }

    public DecoderException(String message) {
        super(message);

    }

    public DecoderException(Throwable cause) {
        super(cause);

    }

}
