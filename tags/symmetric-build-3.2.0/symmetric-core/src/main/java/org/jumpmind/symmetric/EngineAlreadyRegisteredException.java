package org.jumpmind.symmetric;

public class EngineAlreadyRegisteredException extends SymmetricException {

    private static final long serialVersionUID = 1L;

    public EngineAlreadyRegisteredException() {
    }

    public EngineAlreadyRegisteredException(Throwable cause) {
        super(cause);
    }

    public EngineAlreadyRegisteredException(String message, Object... args) {
        super(message, args);
    }

    public EngineAlreadyRegisteredException(String message, Throwable cause) {
        super(message, cause);
    }

    public EngineAlreadyRegisteredException(String message, Throwable cause, Object... args) {
        super(message, cause, args);    }

}
