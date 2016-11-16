package org.jumpmind.symmetric.service;

import org.jumpmind.properties.TypedProperties;

public interface IMailService {
    
    public String sendEmail(String subject, String text, String recipients);

    public String sendEmail(String subject, String text, String recipients, TypedProperties prop);
    
    public String testTransport(TypedProperties prop);

}
