
package org.jumpmind.symmetric.transport;

import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.util.List;
import java.util.Map;

import org.jumpmind.symmetric.model.BatchAck;
import org.jumpmind.symmetric.model.IncomingBatch;
import org.jumpmind.symmetric.model.Node;

public class MockTransportManager implements ITransportManager {

    protected IIncomingTransport incomingTransport;

    protected IOutgoingWithResponseTransport outgoingTransport;

    public String resolveURL(String url, String registrationUrl) {
        return null;
    }

    public void addExtensionSyncUrlHandler(String name, ISyncUrlExtension handler) {
    }

    public IIncomingTransport getPullTransport(Node remote, Node local,
            String securityToken, Map<String, String> requestProperties, String registrationUrl)
            throws IOException {
        return incomingTransport;
    }
    
    public IIncomingTransport getFilePullTransport(Node remote, Node local, String securityToken,
            Map<String, String> requestProperties, String registrationUrl) throws IOException {
        return incomingTransport;
    }

    public IOutgoingWithResponseTransport getPushTransport(Node remote,
        Node local, String securityToken, String registrationUrl) throws IOException {
        return outgoingTransport;
    }

    public int sendAcknowledgement(Node remote, List<IncomingBatch> list,
                Node local, String securityToken, String registrationUrl) throws IOException {
        return HttpURLConnection.HTTP_OK;
    }

    public void writeAcknowledgement(OutputStream out, Node remote,
            List<IncomingBatch> list, Node local, String securityToken)
            throws IOException {
    }

    public IIncomingTransport getIncomingTransport() {
        return incomingTransport;
    }

    public void setIncomingTransport(IIncomingTransport is) {
        this.incomingTransport = is;
    }

    public IOutgoingWithResponseTransport getOutgoingTransport() {
        return outgoingTransport;
    }

    public void setOutgoingTransport(IOutgoingWithResponseTransport outgoingTransport) {
        this.outgoingTransport = outgoingTransport;
    }

    public IIncomingTransport getRegisterTransport(Node node, String registrationUrl) throws IOException {
        return incomingTransport;
    }

    public List<BatchAck> readAcknowledgement(String parameterString) throws IOException {
        return null;
    }

    public List<BatchAck> readAcknowledgement(Map<String, Object> parameters) {
        return null;
    }

    public List<BatchAck> readAcknowledgement(String parameterString1, String parameterString2) throws IOException {
        return null;
    }
    
    public IOutgoingWithResponseTransport getFilePushTransport(Node remote, Node local,
            String securityToken, String registrationUrl) throws IOException {
        return outgoingTransport;
    }



}