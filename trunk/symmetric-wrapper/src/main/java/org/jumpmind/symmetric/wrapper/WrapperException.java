package org.jumpmind.symmetric.wrapper;

public class WrapperException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    private int errorCode;
    
    private int nativeErrorCode;
    
    private String message;
    
    private Exception cause;

    public WrapperException(int errorCode, int nativeErrorCode, String message) {
        this.errorCode = errorCode;
        this.nativeErrorCode = nativeErrorCode;
        this.message = message;
    }
    
    public WrapperException(int errorCode, int nativeErrorCode, String message, Exception cause) {
        this.cause = cause;
    }

    @Override
    public String toString() {
        String s = "Error " + errorCode + " [" + message + "]";
        if (nativeErrorCode > 0) {
            s += " with native error " + nativeErrorCode;
        }
        if (cause != null) {
            s += " caused by " + cause.getClass().getSimpleName() + " [" + cause.getMessage() + "]";
        }
        return s;
    }

    public int getErrorCode() {
        return errorCode;
    }

    public int getNativeErrorCode() {
        return nativeErrorCode;
    }

    public String getMessage() {
        return message;
    }

    @Override
    public Exception getCause() {
        return cause;
    }
}
