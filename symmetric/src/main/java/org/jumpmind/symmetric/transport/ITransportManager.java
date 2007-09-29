package org.jumpmind.symmetric.transport;

import java.io.IOException;
import java.io.OutputStream;
import java.util.List;

import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.model.IncomingBatchHistory;

public interface ITransportManager {

    public boolean sendAcknowledgement(Node remote, List<IncomingBatchHistory> list, Node local) throws IOException;

    public void writeAcknowledgement(OutputStream out, List<IncomingBatchHistory> list) throws IOException;

    public IIncomingTransport getPullTransport(Node remote, Node local) throws IOException;
    
    public IOutgoingWithResponseTransport getPushTransport(Node remote, Node local) throws IOException;

    public IIncomingTransport getRegisterTransport(Node client) throws IOException;
    
}
