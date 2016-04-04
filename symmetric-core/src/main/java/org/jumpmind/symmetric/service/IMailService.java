package org.jumpmind.symmetric.service;

import org.jumpmind.properties.TypedProperties;

public interface IMailService {
    
    public void sendEmail(String subject, String text, String recipients);
    
    public String testTransport(TypedProperties prop);

}
