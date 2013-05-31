
package org.jumpmind.symmetric.transport;

import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;

import org.jumpmind.symmetric.model.BatchAck;
import org.jumpmind.symmetric.model.IncomingBatch;
import org.jumpmind.symmetric.model.Node;

public interface ITransportManager {

    public int sendAcknowledgement(Node remote, List<IncomingBatch> list, Node local, String securityToken, String registrationUrl) throws IOException;

    public void writeAcknowledgement(OutputStream out, Node remote, List<IncomingBatch> list, Node local, String securityToken) throws IOException;

    public List<BatchAck> readAcknowledgement(String parameterString1, String parameterString2) throws IOException;
    
    public IIncomingTransport getFilePullTransport(Node remote, Node local, String securityToken,
            Map<String, String> requestProperties, String registrationUrl) throws IOException;   
    
    public IOutgoingWithResponseTransport getFilePushTransport(Node remote, Node local,
            String securityToken, String registrationUrl) throws IOException;

    public IIncomingTransport getPullTransport(Node remote, Node local, String securityToken, Map<String,String> requestProperties, String registrationUrl) throws IOException;

    public IOutgoingWithResponseTransport getPushTransport(Node remote, Node local, String securityToken, String registrationUrl) throws IOException;

    public IIncomingTransport getRegisterTransport(Node node, String registrationUrl) throws IOException;
    
    public void addExtensionSyncUrlHandler(String name, ISyncUrlExtension handler);
    
    /**
     * This is the proper way to determine the URL for a node.  It delegates to configured 
     * extension points when necessary to take in to account custom load balancing and
     * url selection schemes.
     * @param url This is the url configured in sync_url of the node table
     */
    public String resolveURL(String url, String registrationUrl);

}