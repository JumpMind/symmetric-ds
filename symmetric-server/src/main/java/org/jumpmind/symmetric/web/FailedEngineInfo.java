package org.jumpmind.symmetric.web;

public class FailedEngineInfo {
    private String engineName;
    private String propertyFileName;
    private String errorMessage;
    private Throwable exception;

    public FailedEngineInfo() {
    }

    public FailedEngineInfo(String engineName, String propertyFileName, String errorMessage, Throwable exception) {
        this.engineName = engineName;
        this.propertyFileName = propertyFileName;
        this.exception = exception;
        this.errorMessage = errorMessage;
        if (errorMessage == null && exception != null) {
            StringBuilder sb = new StringBuilder("Failed to initialize engine");
            Throwable t = exception;
            do {
                sb.append(", [").append(t.getClass().getSimpleName()).append(": ").append(t.getMessage()).append("]");
                t = t.getCause();
            } while (t != null);
            this.errorMessage = sb.toString();
        }
    }

    public FailedEngineInfo(String engineName, String propertyFileName, Throwable exception) {
        this(engineName, propertyFileName, null, exception);
    }

    public FailedEngineInfo(String engineName, String propertyFileName, String errorMessage) {
        this(engineName, propertyFileName, errorMessage, null);
    }

    public String getEngineName() {
        return engineName;
    }

    public void setEngineName(String engineName) {
        this.engineName = engineName;
    }

    public String getPropertyFileName() {
        return propertyFileName;
    }

    public void setPropertyFileName(String propertyFileName) {
        this.propertyFileName = propertyFileName;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public void setErrorMessage(String errorMessage) {
        this.errorMessage = errorMessage;
    }

    public Throwable getException() {
        return exception;
    }

    public void setException(Throwable exception) {
        this.exception = exception;
    }
}
