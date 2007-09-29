package org.jumpmind.symmetric.service;

import java.io.IOException;
import java.io.OutputStream;

import org.jumpmind.symmetric.model.Node;

public interface IRegistrationService {
   
    public boolean registerNode(Node client, OutputStream out) throws IOException;
    
    public void openRegistration(String domainName, String domainId);
    
    public void reOpenRegistration(String clientId);

}
