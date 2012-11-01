package org.jumpmind.symmetric.web.rest.model;

import javax.xml.bind.annotation.XmlRootElement;

import org.apache.commons.lang.exception.ExceptionUtils;

@XmlRootElement(name="error")
public class RestError {

    protected String message;
    protected String details;
    protected int statusCode;
    
    public RestError() {     
    }

    public RestError(Exception ex, int statusCode) {
        this.message = ex.getMessage();
        this.statusCode = statusCode;
        this.details = ExceptionUtils.getFullStackTrace(ex);
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public int getStatusCode() {
        return statusCode;
    }

    public void setStatusCode(int statusCode) {
        this.statusCode = statusCode;
    }
    
    public void setDetails(String details) {
        this.details = details;
    }
    
    public String getDetails() {
        return details;
    }

}
