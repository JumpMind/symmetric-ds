package org.jumpmind.symmetric.service;

import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.model.OutgoingBatch;
import org.jumpmind.symmetric.model.Trigger;
import org.jumpmind.symmetric.transport.IOutgoingTransport;

public interface IDataExtractorService {

    public void extractClientIdentityFor(Node client,
            IOutgoingTransport transport);

    public OutgoingBatch extractInitialLoadFor(Node client, Trigger config,
            IOutgoingTransport transport);

    /**
     * @return true if work was done or false if there was no work to do.
     */
    public boolean extract(Node client, IOutgoingTransport transport)
            throws Exception;

    public boolean extract(Node client, final IExtractListener handler)
            throws Exception;

}
