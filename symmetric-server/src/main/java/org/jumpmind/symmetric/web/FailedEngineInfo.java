package org.jumpmind.symmetric.web;

public class FailedEngineInfo {

	private String engineName;
	
	private String propertyFileName;
	
	private String errorMessage;

	public FailedEngineInfo() {
	}

	public FailedEngineInfo(String engineName, String propertyFileName, String errorMessage) {
		this.engineName = engineName;
		this.propertyFileName = propertyFileName;
		this.errorMessage = errorMessage;
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

}
