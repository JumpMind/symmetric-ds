package org.jumpmind.symmetric;

import org.jumpmind.symmetric.SymmetricException;

public class SyntaxParsingException extends SymmetricException {

    private static final long serialVersionUID = 1L;

    public SyntaxParsingException() {
    }

    public SyntaxParsingException(Throwable cause) {
        super(cause);
    }

    public SyntaxParsingException(String message, Object... args) {
        super(message, args);
    }

    public SyntaxParsingException(String message, Throwable cause) {
        super(message, cause);
    }

    public SyntaxParsingException(String message, Throwable cause, Object... args) {
        super(message, cause, args);    }

}
