package org.jumpmind.symmetric.service;


/**
 * Indicate that there is a different node that regsitry should happen with.
 */
public class RegistrationRedirectException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    private String redirectionUrl;

    public RegistrationRedirectException(String redirectUrl) {
        this.redirectionUrl = redirectUrl;
    }

    public void setRedirectionUrl(String redirectionUrl) {
        this.redirectionUrl = redirectionUrl;
    }

    public String getRedirectionUrl() {
        return redirectionUrl;
    }
}