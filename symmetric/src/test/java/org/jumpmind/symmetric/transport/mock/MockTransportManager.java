package org.jumpmind.symmetric.transport.mock;

import java.io.IOException;
import java.io.OutputStream;
import java.util.List;

import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.model.IncomingBatchHistory;
import org.jumpmind.symmetric.transport.IIncomingTransport;
import org.jumpmind.symmetric.transport.IOutgoingWithResponseTransport;
import org.jumpmind.symmetric.transport.ITransportManager;

public class MockTransportManager implements ITransportManager {

    protected IIncomingTransport incomingTransport;
    
    protected IOutgoingWithResponseTransport outgoingTransport;
    
    public IIncomingTransport getPullTransport(Node remote, Node local) throws IOException {
        return incomingTransport;
    }

    public IOutgoingWithResponseTransport getPushTransport(Node remote, Node local) throws IOException {
        return outgoingTransport;
    }

    public boolean sendAcknowledgement(Node remote, List<IncomingBatchHistory> list, Node local) throws IOException {
        return true;
    }

    public boolean sendMessage(Node client, String data) throws IOException {
        return true;
    }

    public void writeAcknowledgement(OutputStream out, List<IncomingBatchHistory> list) throws IOException {
    }

    public void writeMessage(OutputStream out, String data) throws IOException {  
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

    public IIncomingTransport getRegisterTransport(Node client) throws IOException {
        return incomingTransport;
    }

}
