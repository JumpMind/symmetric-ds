package org.jumpmind.symmetric.web.rest.model;

import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement
public class ActionResponse {

    protected boolean success;
    protected String message;

    public boolean isSuccess() {
        return success;
    }

    public void setSuccess(boolean success) {
        this.success = success;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

}
